// Copyright 2025 Isaac Blankenau
// Copyright 2026 Robot.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from rosetta's episode_recorder_node.py (Apache-2.0,
// Copyright 2025 Isaac Blankenau). See NOTICE for what changed.

#include "rosetta_recorder_cpp/contract.hpp"

#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

#include "rmw/types.h"
#include "yaml-cpp/yaml.h"

namespace rosetta_recorder_cpp
{

const char * to_string(BufferingStrategy s)
{
  switch (s) {
    case BufferingStrategy::NoBuffer: return "no_buffer";
    case BufferingStrategy::Accumulate: return "accumulate";
    case BufferingStrategy::ResubscribeOnStart: return "resubscribe_on_start";
  }
  return "no_buffer";
}

namespace
{

/// rclcpp::Duration from the (sec, nsec) pair rosbag2 stores. The "infinite"
/// pair 2147483647 / 4294967295 is RMW_DURATION_INFINITE exactly.
rclcpp::Duration duration_from(int64_t sec, int64_t nsec)
{
  return rclcpp::Duration(
    static_cast<int32_t>(sec), static_cast<uint32_t>(nsec));
}

}  // namespace

rclcpp::QoS QosSpec::to_rclcpp() const
{
  rclcpp::QoS qos = keep_last
    ? rclcpp::QoS(rclcpp::KeepLast(static_cast<size_t>(depth)))
    : rclcpp::QoS(rclcpp::KeepAll());
  if (reliable) {
    qos.reliable();
  } else {
    qos.best_effort();
  }
  if (transient_local) {
    qos.transient_local();
  } else {
    qos.durability_volatile();
  }
  if (match_publisher_profile) {
    // record_all only: request exactly what the publisher offers, the way the
    // Python node does by reusing the publisher's QoSProfile verbatim. Contract
    // topics deliberately keep rclcpp's defaults, which is what rclpy's
    // `qos=<QoSProfile from the contract>` resolves to.
    qos.deadline(duration_from(deadline_sec, deadline_nsec));
    qos.lifespan(duration_from(lifespan_sec, lifespan_nsec));
    qos.liveliness(static_cast<rclcpp::LivelinessPolicy>(liveliness));
    qos.liveliness_lease_duration(
      duration_from(liveliness_lease_sec, liveliness_lease_nsec));
  }
  return qos;
}

rclcpp::QoS QosSpec::to_offered_qos() const
{
  // Same values as to_offered_qos_yaml(), in the object form Jazzy's
  // TopicMetadata wants — including the fields to_rclcpp() deliberately leaves
  // at rclcpp's defaults for contract topics.
  QosSpec full = *this;
  full.match_publisher_profile = true;
  return full.to_rclcpp();
}

std::string QosSpec::to_offered_qos_yaml() const
{
  // rosbag2 on Humble stores offered_qos_profiles as a YAML string. The player
  // requires every field to be present, so emit them all. "Infinite" durations
  // are encoded the way rosbag2 itself encodes them.
  const int history = keep_last
    ? RMW_QOS_POLICY_HISTORY_KEEP_LAST
    : RMW_QOS_POLICY_HISTORY_KEEP_ALL;
  const int reliability = reliable
    ? RMW_QOS_POLICY_RELIABILITY_RELIABLE
    : RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT;
  const int durability = transient_local
    ? RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL
    : RMW_QOS_POLICY_DURABILITY_VOLATILE;
  // Defaults to SYSTEM_DEFAULT, not AUTOMATIC: contract QoS never specifies
  // liveliness, so this is what the subscription actually offers, and it is what
  // the Python recorder wrote into every bag produced so far. Only a discovered
  // publisher can move it off the default.

  std::ostringstream os;
  os << "- history: " << history << "\n"
     << "  depth: " << depth << "\n"
     << "  reliability: " << reliability << "\n"
     << "  durability: " << durability << "\n"
     << "  deadline:\n"
     << "    sec: " << deadline_sec << "\n"
     << "    nsec: " << deadline_nsec << "\n"
     << "  lifespan:\n"
     << "    sec: " << lifespan_sec << "\n"
     << "    nsec: " << lifespan_nsec << "\n"
     << "  liveliness: " << liveliness << "\n"
     << "  liveliness_lease_duration:\n"
     << "    sec: " << liveliness_lease_sec << "\n"
     << "    nsec: " << liveliness_lease_nsec << "\n"
     << "  avoid_ros_namespace_conventions: false";
  return os.str();
}

namespace
{

std::string lower_trim(const std::string & in)
{
  size_t b = in.find_first_not_of(" \t\r\n");
  if (b == std::string::npos) {
    return "";
  }
  size_t e = in.find_last_not_of(" \t\r\n");
  std::string s = in.substr(b, e - b + 1);
  std::transform(
    s.begin(), s.end(), s.begin(),
    [](unsigned char c) {return static_cast<char>(std::tolower(c));});
  return s;
}

/// Read a required scalar string, with a contract-style error message.
std::string require_str(const YAML::Node & n, const char * field, const std::string & ctx)
{
  if (!n[field] || n[field].IsNull()) {
    throw ContractValidationError(
      std::string("Missing required field '") + field + "' in " + ctx);
  }
  const auto v = n[field].as<std::string>("");
  if (v.empty()) {
    throw ContractValidationError(std::string("Empty ") + field + " in " + ctx);
  }
  return v;
}

QosSpec parse_qos(const YAML::Node & n)
{
  // Absent or empty `qos:` keeps the defaults, which is what the Python node's
  // `qos_profile_from_dict(...) or 10` fallback resolves to — with one wrinkle:
  // that fallback hands rosbag2 a bare int, and extract_qos_numeric_values()
  // reports AUTOMATIC liveliness for an int while reporting the QoSProfile's
  // SYSTEM_DEFAULT for everything else. Reproduce it so bag metadata matches
  // for specs that omit `qos:`.
  QosSpec q;
  // An empty mapping takes the same path: `qos_profile_from_dict({})` is falsy
  // in Python and falls through to the bare `10`.
  if (!n || !n.IsMap() || n.size() == 0) {
    q.liveliness = RMW_QOS_POLICY_LIVELINESS_AUTOMATIC;
    return q;
  }
  if (n["reliability"]) {
    q.reliable = lower_trim(n["reliability"].as<std::string>("reliable")) != "best_effort";
  }
  if (n["history"]) {
    q.keep_last = lower_trim(n["history"].as<std::string>("keep_last")) != "keep_all";
  }
  if (n["durability"]) {
    q.transient_local =
      lower_trim(n["durability"].as<std::string>("volatile")) == "transient_local";
  }
  if (n["depth"]) {
    q.depth = n["depth"].as<int>(10);
  }
  if (q.depth <= 0) {
    q.depth = 10;
  }
  return q;
}

/// observations / rewards / signals / info / complementary_data all expose
/// topic+type+qos at the top level.
void collect_observation_like(
  const YAML::Node & list, const char * section, std::vector<RecordTopic> & out)
{
  if (!list || !list.IsSequence()) {
    return;
  }
  for (size_t i = 0; i < list.size(); ++i) {
    const auto & item = list[i];
    const std::string ctx = std::string(section) + "[" + std::to_string(i) + "]";
    if (!item.IsMap()) {
      throw ContractValidationError(ctx + " must be a mapping");
    }
    RecordTopic t;
    t.topic = require_str(item, "topic", ctx);
    t.type = require_str(item, "type", ctx);
    t.qos = parse_qos(item["qos"]);
    t.strategy = BufferingStrategy::NoBuffer;
    out.push_back(std::move(t));
  }
}

/// actions nest the topic under `publish:`.
void collect_actions(const YAML::Node & list, std::vector<RecordTopic> & out)
{
  if (!list || !list.IsSequence()) {
    return;
  }
  for (size_t i = 0; i < list.size(); ++i) {
    const auto & item = list[i];
    const std::string ctx = "actions[" + std::to_string(i) + "]";
    if (!item.IsMap()) {
      throw ContractValidationError(ctx + " must be a mapping");
    }
    const auto & pub = item["publish"];
    if (!pub || !pub.IsMap()) {
      throw ContractValidationError("'publish' must be a mapping in " + ctx);
    }
    RecordTopic t;
    t.topic = require_str(pub, "topic", ctx + ".publish");
    t.type = require_str(pub, "type", ctx + ".publish");
    t.qos = parse_qos(pub["qos"]);
    t.strategy = BufferingStrategy::NoBuffer;
    out.push_back(std::move(t));
  }
}

void collect_tasks(const YAML::Node & list, std::vector<RecordTopic> & out)
{
  if (!list || !list.IsSequence()) {
    return;
  }
  for (size_t i = 0; i < list.size(); ++i) {
    const auto & item = list[i];
    const std::string ctx = "tasks[" + std::to_string(i) + "]";
    RecordTopic t;
    t.topic = require_str(item, "topic", ctx);
    t.type = require_str(item, "type", ctx);
    t.qos = parse_qos(item["qos"]);
    t.strategy = BufferingStrategy::NoBuffer;
    out.push_back(std::move(t));
  }
}

RecordTopic parse_adjunct(const YAML::Node & item, size_t idx)
{
  const std::string ctx = "adjunct[" + std::to_string(idx) + "]";
  if (!item.IsMap()) {
    throw ContractValidationError(ctx + " must be a mapping");
  }
  RecordTopic t;
  t.topic = require_str(item, "topic", ctx);
  t.type = require_str(item, "type", ctx);
  t.qos = parse_qos(item["qos"]);

  if (item["buffering_strategy"] && !item["buffering_strategy"].IsNull()) {
    const std::string s = lower_trim(item["buffering_strategy"].as<std::string>(""));
    if (s == "no_buffer") {
      t.strategy = BufferingStrategy::NoBuffer;
    } else if (s == "accumulate") {
      t.strategy = BufferingStrategy::Accumulate;
    } else if (s == "resubscribe_on_start") {
      t.strategy = BufferingStrategy::ResubscribeOnStart;
    } else {
      throw ContractValidationError(
        "Invalid buffering_strategy '" + s + "' in " + ctx +
        ". Must be one of: [accumulate, no_buffer, resubscribe_on_start]");
    }
    if (t.strategy != BufferingStrategy::NoBuffer && !t.qos.transient_local) {
      throw ContractValidationError(
        "buffering_strategy '" + s + "' can only be used with transient_local "
        "durability in " + ctx);
    }
  } else {
    // Auto-detect, same rule as the Python node: latched topics accumulate so
    // their sample lands at the head of the bag, everything else drops.
    t.strategy = t.qos.transient_local
      ? BufferingStrategy::Accumulate
      : BufferingStrategy::NoBuffer;
  }
  return t;
}

}  // namespace

Contract load_contract(const std::string & path)
{
  YAML::Node root;
  try {
    root = YAML::LoadFile(path);
  } catch (const YAML::BadFile &) {
    throw ContractValidationError("Contract file not found: " + path);
  } catch (const YAML::Exception & e) {
    throw ContractValidationError("Invalid YAML in " + path + ": " + e.what());
  }

  if (!root.IsMap()) {
    throw ContractValidationError("Contract must be a YAML mapping");
  }

  Contract c;
  if (!root["robot_type"] || root["robot_type"].as<std::string>("").empty()) {
    throw ContractValidationError("robot_type is required");
  }
  c.robot_type = root["robot_type"].as<std::string>();

  c.fps = root["fps"] ? root["fps"].as<int>(30) : 30;
  if (c.fps <= 0) {
    throw ContractValidationError("fps must be positive, got " + std::to_string(c.fps));
  }
  c.max_duration_s = root["max_duration_s"] ? root["max_duration_s"].as<double>(30.0) : 30.0;

  if (root["recording"] && root["recording"].IsMap() && root["recording"]["storage"]) {
    c.storage = root["recording"]["storage"].as<std::string>("");
  }

  // Order matters: it fixes the bag's topic ids. Mirrors the Python node's
  // _build_topic_list -> iter_specs (observations, actions, extended), tasks,
  // adjunct.
  collect_observation_like(root["observations"], "observations", c.topics);
  collect_actions(root["actions"], c.topics);
  collect_observation_like(root["rewards"], "rewards", c.topics);
  collect_observation_like(root["signals"], "signals", c.topics);
  collect_observation_like(root["info"], "info", c.topics);
  collect_observation_like(root["complementary_data"], "complementary_data", c.topics);
  collect_tasks(root["tasks"], c.topics);

  const auto & adj = root["adjunct"];
  if (adj && !adj.IsNull()) {
    if (adj.IsSequence()) {
      for (size_t i = 0; i < adj.size(); ++i) {
        c.topics.push_back(parse_adjunct(adj[i], i));
      }
    } else {
      c.topics.push_back(parse_adjunct(adj, 0));
    }
  }

  return c;
}

}  // namespace rosetta_recorder_cpp
