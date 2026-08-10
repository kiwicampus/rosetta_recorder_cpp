# rosetta_recorder_cpp

A C++ episode recorder for [rosetta](https://github.com/iblnkn/rosetta): a
lifecycle node that streams the topics named by a rosetta contract straight into
a rosbag2 MCAP bag, under action and service control.

It is a drop-in replacement for rosetta's `episode_recorder_node` using same node
name, same action, same services, same parameters, same bag layout and metadata.

## Why

rclpy has a known high CPU usage problem on high-rate topics, specially on not very powerful CPUs, and a recorder is
exactly the workload that provokes it.

rosetta's rclpy recorder deserializes every incoming message into a Python
object, then immediately re-serializes it back to CDR to write it to the bag.
Both conversions are pure waste for a recorder, and they are paid **whether or
not it is recording**, because the "am I recording?" check happens after rclpy
has already deserialized.

### The workload these numbers come from

A recorder's cost tracks message **count**, not payload, so the traffic matters
more than the hardware. All figures below were measured on a Jetson Orin
(aarch64, 9 cores, load avg ~10), ROS 2 Humble, `rmw_fastrtps_cpp`, against a
14-topic contract carrying **1,930 msg/s / 16.08 MiB/s**:

| Topics | Kind | Rate each | Mean size | Share of msgs | Share of bytes |
|---|---|---:|---:|---:|---:|
| 7 | `sensor_msgs/msg/JointState` | 250 Hz | 84–276 B | **90.7%** | 1.7% |
| 1 | `sensor_msgs/msg/CompressedImage` | 60 Hz | 97 KB | 3.1% | 34.5% |
| 4 | `sensor_msgs/msg/CompressedImage` | 30 Hz | 54–147 KB | 3.1% | 63.8% |
| 2 | `std_msgs/msg/String`, latched | latched | — | — | — |

Note the split: the 250 Hz joint states are **90.7% of the messages and 1.7% of
the bytes**. A 276-byte joint state costs a recorder nearly what a 147 KB image
does which is why per-message overhead, not bandwidth, is what separates these
implementations.

Measure your own before comparing: `tools/contract_traffic.py` prints exactly
this table for any contract:

```bash
python3 tools/contract_traffic.py --contract /path/to/contract.yaml --duration 30
```

### Results

Measured over one window with both processes running:

| | CPU (1 core = 100%) | RSS |
|---|---|---|
| rclpy recorder, **idle** | 95.3% | 359 MiB |
| rclpy recorder, **actively writing** | 95.5% | 359 MiB |
| this node, **idle** | **23.1%** | 28 MiB |
| this node, **actively writing** (~15 MiB/s) | 28.5% | 28 MiB |

Recording in C++ costs less than a third of what the Python node costs doing nothing.

Two things account for it. Generic subscriptions (`rclcpp::GenericSubscription`)
keep messages as CDR from the wire to the bag, so nothing is ever deserialized;
and a single-threaded executor beats a multi-threaded one for this workload,
measurably, because the work is many tiny messages rather than a few large ones.

Part of this is recoverable in Python without a rewrite by just adding `raw=True`
to subscriptions on a single-threaded executor. This measured a ~27-point CPU drop, but
the remainder is rclpy's per-message dispatch, which is what this package exists
to avoid; those results will be written up in a rosetta issue.

Reproduce it on your own workload with `tools/rclpy_raw_probe.py` (needs
`rclpy`; not installed by the build, it is a diagnostic script):

```bash
python3 tools/rclpy_raw_probe.py --contract <path> --mode raw --threads 0 \
  --duration 30 --write /tmp/probe_bags
```

## Executor threads

This node runs a **single-threaded** executor, which is usually the first thing
people question: surely one thread drops messages under load?

Measured, it is the other way round. A recorder's callbacks are short (copy
bytes, enqueue) so extra threads add wakeups and lock traffic without adding
throughput, and the contention costs enough that the multi-threaded
configuration falls *behind*. In rclpy the GIL serializes the callbacks anyway,
so the threads never run in parallel to begin with; in C++ they genuinely could,
and it is still slower.

What actually protects against drops is not thread count but never blocking the
one thread you have. This node therefore does disk I/O on a dedicated writer
thread fed by a bounded queue, and drives the episode from a timer rather than a
sleeping loop, so a slow flush cannot stall message reception. Queue overruns are
counted, logged, and reported in the action result rather than silently lost. In practice, 0 drops across 135k-message episodes at ~15 MiB/s.

## Build

```bash
colcon build --packages-select rosetta_recorder_cpp \
  --cmake-args -DCMAKE_BUILD_TYPE=Release
```

Depends on `rosetta_interfaces` for the `RecordEpisode` action and the
`StartRecording` service. `rosbag2_storage_mcap` is a runtime dependency of the
default `mcap` storage backend.

ROS 2 Humble and Jazzy are verified.

## Run

```bash
ros2 launch rosetta_recorder_cpp rosetta_recorder_cpp.launch.py \
  contract_path:=/path/to/contract.yaml \
  bag_base_dir:=/path/to/bags
```

The launch file brings the node up and drives it configure → activate. To run it
by hand:

```bash
ros2 run rosetta_recorder_cpp episode_recorder_node --ros-args \
  -p contract_path:=/path/to/contract.yaml
ros2 lifecycle set /episode_recorder configure
ros2 lifecycle set /episode_recorder activate
```

### Recording

```bash
# via the action, with feedback
ros2 action send_goal /record_episode rosetta_interfaces/action/RecordEpisode \
  "{prompt: 'pick up the cube'}" --feedback

# or via services, for clients that cannot reach the hidden _action/* services
ros2 service call /episode_recorder/start_recording \
  rosetta_interfaces/srv/StartRecording "{prompt: 'pick up the cube'}"
ros2 service call /episode_recorder/cancel_recording std_srvs/srv/Trigger
ros2 service call /episode_recorder/delete_last_bag std_srvs/srv/Trigger
```

An episode ends **SUCCEEDED** when it runs to `default_max_duration`, and
**CANCELED** when it is stopped by the action's cancel, or by
`~/cancel_recording`. Consumers use that distinction to tell an intended stop
from an unintended one, so it is part of the interface, not an implementation
detail.

## Parameters

| Parameter | Default | |
|---|---|---|
| `contract_path` | `""` | Contract YAML. Required. Read-only. |
| `bag_base_dir` | `/workspaces/rosetta_ws/datasets/bags` | Where bags are written. Read-only. |
| `storage_id` | `mcap` | rosbag2 storage plugin. Read-only. |
| `exclude_topics` | `[""]` | Regexes excluded from `record_all`, same syntax as `ros2 bag record --exclude`. Read-only. |
| `record_all` | `false` | Also record non-contract topics found on the graph. Read-only. |
| `bag_name_style` | `epoch` | Bag directory naming. `epoch` is rosetta's `<epoch_sec>_<nsec>`; `datetime` is `YYYYMMDD-HHMMSS-mmm` in local time. Any other value fails `configure`. Read-only. |
| `default_max_duration` | `300.0` | Seconds before an episode auto-stops. |
| `feedback_rate_hz` | `2.0` | Action feedback rate; also how often a stop request is noticed. |

## What it reads from a contract

Only what recording needs: `topic`, `type`, `qos` and `buffering_strategy`, from
`observations`, `actions[].publish`, `rewards`, `signals`, `info`,
`complementary_data`, `tasks` and `adjunct` in that order, because the order
fixes the bag's topic ids.

It does **not** validate LeRobot dtypes, image `resize`, or that
`decoder:`/`encoder:` paths are importable; that needs Python and is irrelevant
to writing bytes to a bag. A contract this node happily records may still fail
later in conversion, so keep the Python-side validation in your converter.

`buffering_strategy` on a latched (`transient_local`) adjunct topic controls what
happens to messages that arrive while not recording:

- `no_buffer` — dropped (the default for anything not latched).
- `accumulate` — up to `depth` messages retained and written at t=0 of the bag,
  so a player has them immediately. The default for latched topics.
- `resubscribe_on_start` — the subscription is torn down and recreated when
  recording starts, so the publisher re-delivers its latched sample into a fresh
  buffer.

## Tests

```bash
colcon build --packages-select rosetta_recorder_cpp \
  --cmake-args -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
colcon test --packages-select rosetta_recorder_cpp --event-handlers console_direct+
```

42 tests: contract loading and QoS metadata (asserted byte-for-byte against what
the Python recorder emits, so bags stay readable by the same tools), and
node-level tests covering the lifecycle, both start paths, all three terminal
states, latched-message buffering, and two end-to-end recordings that are read
back off disk. They run on their own `ROS_DOMAIN_ID`.

## License

Apache-2.0. This package is a port of rosetta's `episode_recorder_node.py`
(Apache-2.0, Copyright 2025 Isaac Blankenau); see `NOTICE` for attribution and
the list of changes.
