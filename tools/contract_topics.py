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
# Contract reading shared by the measurement tools in this directory. The
# recorder itself parses contracts in C++ (src/contract.cpp); this is the
# rclpy-side equivalent, kept in one place so two probes measuring the same
# contract cannot disagree about what it names.

from typing import NamedTuple

from rclpy.qos import DurabilityPolicy
from rclpy.qos import HistoryPolicy
from rclpy.qos import QoSProfile
from rclpy.qos import ReliabilityPolicy
import yaml

# Contract sections holding a flat list of {topic, type, qos} entries.
_LIST_SECTIONS = ("rewards", "signals", "info", "complementary_data", "tasks")


class TopicSpec(NamedTuple):
    """One recordable topic, as named by a contract."""

    section: str
    topic: str
    msg_type: str
    qos: QoSProfile


def qos_from_dict(d: dict | None) -> QoSProfile:
    """Build a QoSProfile from a contract's qos mapping.

    Supports the same four keys as rosetta's qos_profile_from_dict, and falls
    back to the same defaults for any key that is absent or unrecognized.

    Args:
        d: The contract entry's ``qos`` mapping, or None when it omits one.

    Returns:
        The profile to subscribe with.
    """
    d = d or {}
    reliability = str(d.get("reliability", "reliable")).lower()
    history = str(d.get("history", "keep_last")).lower()
    durability = str(d.get("durability", "volatile")).lower()
    return QoSProfile(
        depth=int(d.get("depth", 10)),
        reliability=(
            ReliabilityPolicy.BEST_EFFORT
            if reliability == "best_effort"
            else ReliabilityPolicy.RELIABLE
        ),
        history=(
            HistoryPolicy.KEEP_ALL if history == "keep_all" else HistoryPolicy.KEEP_LAST
        ),
        durability=(
            DurabilityPolicy.TRANSIENT_LOCAL
            if durability == "transient_local"
            else DurabilityPolicy.VOLATILE
        ),
    )


def _spec(section: str, entry: dict) -> TopicSpec:
    """Build one TopicSpec from a contract entry.

    Args:
        section: Name of the contract section the entry came from.
        entry: The entry itself, carrying ``topic``, ``type`` and ``qos``.

    Returns:
        The spec for that entry.
    """
    return TopicSpec(
        section, entry["topic"], entry["type"], qos_from_dict(entry.get("qos"))
    )


def topics_from_contract(path: str) -> list[TopicSpec]:
    """Read a contract and list every topic it names.

    Args:
        path: Path to the contract YAML.

    Returns:
        The topics, in the order the recorder builds its subscriptions.
    """
    with open(path) as f:
        contract = yaml.safe_load(f) or {}

    out: list[TopicSpec] = []
    for item in contract.get("observations") or []:
        out.append(_spec("observations", item))
    for item in contract.get("actions") or []:
        out.append(_spec("actions", item["publish"]))
    for section in _LIST_SECTIONS:
        for item in contract.get(section) or []:
            out.append(_spec(section, item))

    # A single adjunct entry may be given as a mapping rather than a list.
    adjunct = contract.get("adjunct")
    if isinstance(adjunct, dict):
        adjunct = [adjunct]
    for item in adjunct or []:
        out.append(_spec("adjunct", item))

    return out
