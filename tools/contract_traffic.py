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
# Measure what a contract actually pulls off the wire: per-topic rate, mean
# message size and bandwidth. Run this before quoting any CPU number, because
# a recorder's cost is driven by message *count*, and a benchmark without its
# workload stated is not reproducible.
#
#   python3 contract_traffic.py --contract <path> --duration 30
#
# Uses raw subscriptions so it stays cheap enough not to perturb what it is
# measuring.

import argparse
import time
from collections import defaultdict

import rclpy
from rclpy.executors import SingleThreadedExecutor
from rclpy.node import Node
from rosidl_runtime_py.utilities import get_message

from contract_topics import topics_from_contract


class TrafficNode(Node):
    def __init__(self, contract):
        super().__init__("contract_traffic")
        self.count = defaultdict(int)
        self.bytes = defaultdict(int)
        self.specs = topics_from_contract(contract)
        for _section, topic, type_str, qos in self.specs:
            self.create_subscription(
                get_message(type_str), topic,
                lambda data, t=topic: self._on(data, t), qos, raw=True)

    def _on(self, data, topic):
        self.count[topic] += 1
        self.bytes[topic] += len(data)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--contract", required=True)
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--settle", type=float, default=3.0)
    args = ap.parse_args()

    rclpy.init()
    node = TrafficNode(args.contract)
    executor = SingleThreadedExecutor()
    executor.add_node(node)

    import threading

    def sample():
        time.sleep(args.settle)
        c0 = dict(node.count)
        b0 = dict(node.bytes)
        t0 = time.monotonic()
        time.sleep(args.duration)
        dt = time.monotonic() - t0
        c1, b1 = dict(node.count), dict(node.bytes)

        print(f"\nwindow {dt:.1f}s, {len(node.specs)} topics\n")
        print(f"{'section':13} {'topic':58} {'type':42} {'Hz':>8} {'mean B':>9} {'KiB/s':>10}")
        tot_msgs = tot_bytes = 0.0
        for section, topic, type_str, _qos in node.specs:
            n = c1.get(topic, 0) - c0.get(topic, 0)
            b = b1.get(topic, 0) - b0.get(topic, 0)
            tot_msgs += n
            tot_bytes += b
            mean = (b / n) if n else 0
            print(
                f"{section:13} {topic:58} {type_str:42} "
                f"{n / dt:8.1f} {mean:9.0f} {b / dt / 1024:10.1f}")
        print(
            f"\nTOTAL {tot_msgs / dt:.0f} msg/s, "
            f"{tot_bytes / dt / (1 << 20):.2f} MiB/s "
            f"({tot_msgs:.0f} messages in {dt:.1f}s)")
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
