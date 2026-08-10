#!/usr/bin/env python3
# Copyright 2026 Robot.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# How much of rclpy's recorder cost is serialization, and how much is rclpy?
#
# Subscribes to the topics named by a contract, does nothing in the callbacks,
# and reports its own CPU. Four configurations isolate the two candidate causes:
#
#   --mode typed  : callback receives a deserialized Python message object.
#                   This is what episode_recorder_node.py does today, and it
#                   then re-serializes the object to write it to the bag.
#   --mode raw    : create_subscription(..., raw=True). The callback receives
#                   the CDR bytes straight off the wire — no deserialization,
#                   and nothing to re-serialize before writing to the bag.
#                   The rclpy equivalent of rclcpp's GenericSubscription.
#
#   --threads N   : 0 -> SingleThreadedExecutor, N -> MultiThreadedExecutor(N).
#                   The deployed node uses 4.
#
# Topics come from the contract; nothing here is hardcoded.
#
#   ros2 run ... rclpy_raw_probe.py --contract <path> --mode raw --threads 0
#
# Note: empty callbacks measure the floor. A real recorder additionally pays for
# the bag write, which is the same work in either language.

import argparse
import os
import time

import rclpy
from rclpy.executors import MultiThreadedExecutor, SingleThreadedExecutor
from rclpy.node import Node
from rosidl_runtime_py.utilities import get_message

from contract_topics import topics_from_contract

CLK_TCK = os.sysconf("SC_CLK_TCK")


def read_cpu_seconds() -> float:
    """utime + stime of this process, in seconds."""
    with open("/proc/self/stat") as f:
        fields = f.read().split()
    return (int(fields[13]) + int(fields[14])) / CLK_TCK


class ProbeNode(Node):
    """Subscriptions, optionally writing to a bag.

    Without --write the callbacks do nothing, which measures the subscription
    floor. With --write they do what a recorder does: `raw` hands the CDR bytes
    straight to the writer, `typed` re-serializes the message object first, the
    way episode_recorder_node.py does today.
    """

    def __init__(self, contract: str, raw: bool, write_dir: str | None) -> None:
        super().__init__("rclpy_raw_probe")
        self.count = 0
        self.bytes = 0
        self._raw = raw
        self._writer = None

        topics = topics_from_contract(contract)

        if write_dir:
            import rosbag2_py

            bag_path = os.path.join(
                write_dir, f"probe_{'raw' if raw else 'typed'}_{int(time.time())}"
            )
            self._writer = rosbag2_py.SequentialWriter()
            self._writer.open(
                rosbag2_py.StorageOptions(uri=bag_path, storage_id="mcap"),
                rosbag2_py.ConverterOptions(
                    input_serialization_format="cdr", output_serialization_format="cdr"
                ),
            )
            for _section, topic, type_str, _qos in topics:
                # Minimal metadata: this bag is a stopwatch, not an artifact.
                self._writer.create_topic(
                    rosbag2_py.TopicMetadata(topic, type_str, "cdr", "")
                )
            self.bag_path = bag_path

        for _section, topic, type_str, qos in topics:
            msg_cls = get_message(type_str)
            if raw:
                # The callback gets CDR bytes. No Python message object is ever
                # built, and the bytes go straight to the writer.
                self.create_subscription(
                    msg_cls, topic,
                    lambda data, t=topic: self._on_raw(data, t), qos, raw=True)
            else:
                self.create_subscription(
                    msg_cls, topic,
                    lambda msg, t=topic: self._on_typed(msg, t), qos)

        self.get_logger().info(
            f"subscribed to {len(topics)} topics, mode={'raw' if raw else 'typed'}, "
            f"write={'yes' if write_dir else 'no'}"
        )

    def _on_raw(self, data: bytes, topic: str) -> None:
        self.count += 1
        self.bytes += len(data)
        if self._writer is not None:
            self._writer.write(topic, data, self.get_clock().now().nanoseconds)

    def _on_typed(self, msg, topic: str) -> None:
        self.count += 1
        if self._writer is not None:
            from rclpy.serialization import serialize_message

            self._writer.write(
                topic, serialize_message(msg), self.get_clock().now().nanoseconds)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--contract", required=True)
    ap.add_argument("--mode", choices=["typed", "raw"], default="raw")
    ap.add_argument("--threads", type=int, default=0, help="0 = single-threaded executor")
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--settle", type=float, default=5.0, help="discovery warm-up, not measured")
    ap.add_argument(
        "--write", metavar="DIR",
        help="also write every message to an mcap bag under DIR, i.e. measure "
             "the real recording cost rather than the subscription floor")
    args = ap.parse_args()

    rclpy.init()
    node = ProbeNode(args.contract, raw=(args.mode == "raw"), write_dir=args.write)

    executor = (
        SingleThreadedExecutor()
        if args.threads <= 0
        else MultiThreadedExecutor(num_threads=args.threads)
    )
    executor.add_node(node)

    # The executor spins on the main thread, exactly as the deployed node does —
    # spin_once() in a loop would not reproduce MultiThreadedExecutor's dispatch.
    # A sampler thread reads /proc and then breaks the spin.
    import threading

    def sample() -> None:
        time.sleep(args.settle)
        c0, t0, n0 = read_cpu_seconds(), time.monotonic(), node.count
        time.sleep(args.duration)
        c1, t1, n1 = read_cpu_seconds(), time.monotonic(), node.count

        dt = t1 - t0
        rss_mib = 0.0
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss_mib = int(line.split()[1]) / 1024.0

        label = f"{args.mode}/{'single' if args.threads <= 0 else str(args.threads) + 't'}"
        print(
            f"RESULT mode={label} write={'yes' if args.write else 'no'} "
            f"cpu={(c1 - c0) / dt * 100.0:.2f}% rss={rss_mib:.1f}MiB "
            f"msgs={n1 - n0} rate={(n1 - n0) / dt:.0f}/s "
            f"MiB/s={(node.bytes / (1 << 20)) / (t1 - t0 + args.settle):.1f} "
            f"window={dt:.1f}s",
            flush=True,
        )
        if node._writer is not None:
            node._writer.close()
        executor.shutdown()
        rclpy.try_shutdown()

    threading.Thread(target=sample, daemon=True).start()

    try:
        executor.spin()
    except Exception:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
