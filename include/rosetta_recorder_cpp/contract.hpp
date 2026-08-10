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
//
// Recording-oriented view of a rosetta contract YAML.
//
// The Python contract loader (rosetta/common/contract.py) resolves a lot that
// only matters at conversion time: LeRobot dtypes, image resize/encoding,
// importable decoder/encoder paths, selector name lists, alignment policy. A
// recorder needs none of it — it needs (topic, type, QoS, buffering strategy)
// and nothing else. This header models exactly that subset, so the C++ node
// can read the same contract files without depending on Python.

#ifndef ROSETTA_RECORDER_CPP__CONTRACT_HPP_
#define ROSETTA_RECORDER_CPP__CONTRACT_HPP_

#include <stdexcept>
#include <string>
#include <vector>

#include "rclcpp/qos.hpp"

namespace rosetta_recorder_cpp
{

class ContractValidationError : public std::runtime_error
{
public:
  explicit ContractValidationError(const std::string & what)
  : std::runtime_error(what) {}
};

/// Buffering behavior for TRANSIENT_LOCAL (latched) topics.
enum class BufferingStrategy
{
  /// Drop messages that arrive while not recording.
  NoBuffer,
  /// Keep up to history-depth messages and flush them at bag start.
  Accumulate,
  /// Tear down and recreate the subscription when recording starts, so the
  /// publisher re-delivers its latched sample into a fresh buffer.
  ResubscribeOnStart,
};

const char * to_string(BufferingStrategy s);

/// QoS as expressed in contract YAML. Defaults match
/// rosetta.common.ros2_utils.qos_profile_from_dict, and also match the bare
/// `10` the Python node falls back to when a spec has no `qos:` block.
///
/// The first four fields are everything a contract can express —
/// `qos_profile_from_dict` supports exactly reliability/history/durability/depth
/// and nothing else. The remaining fields exist only so `record_all` can carry a
/// discovered publisher's full profile through to the bag metadata, the way the
/// Python node does by reusing the publisher's QoSProfile verbatim. They default
/// to RMW "infinite duration" / SYSTEM_DEFAULT liveliness, which is what the
/// Python node emits for every contract topic, so contract-derived output is
/// byte-identical whether or not this extension is used.
struct QosSpec
{
  /// Nanoseconds meaning "infinite", as rosbag2 encodes it: sec 2147483647,
  /// nsec 4294967295.
  static constexpr int64_t kInfiniteSec = 2147483647;
  static constexpr int64_t kInfiniteNsec = 4294967295;

  int depth = 10;
  bool reliable = true;
  bool keep_last = true;
  bool transient_local = false;

  /// RMW liveliness policy value written into the bag metadata. SYSTEM_DEFAULT
  /// (0) for a spec that carries a `qos:` block — the Python node derives it
  /// from a QoSProfile there. A spec with no `qos:` block goes through Python's
  /// bare-`10` fallback, whose extract_qos_numeric_values() hard-codes
  /// AUTOMATIC (1); parse_qos reproduces that so the metadata stays identical
  /// for those specs too. Note the subscription itself still requests
  /// SYSTEM_DEFAULT in that case, exactly as rclpy's `qos=10` does.
  int liveliness = 0;

  /// True only for topics discovered by record_all, where the profile was read
  /// off a live publisher and the subscription should request all of it.
  bool match_publisher_profile = false;
  int64_t deadline_sec = kInfiniteSec;
  int64_t deadline_nsec = kInfiniteNsec;
  int64_t lifespan_sec = kInfiniteSec;
  int64_t lifespan_nsec = kInfiniteNsec;
  int64_t liveliness_lease_sec = kInfiniteSec;
  int64_t liveliness_lease_nsec = kInfiniteNsec;

  rclcpp::QoS to_rclcpp() const;

  /// `offered_qos_profiles` YAML for rosbag2 topic metadata on Humble.
  /// Byte-compatible with what the Python node writes today, so bags stay
  /// readable by the same players and converters.
  std::string to_offered_qos_yaml() const;

  /// The same profile as an rclcpp::QoS carrying every field, for the
  /// Jazzy-and-newer `std::vector<rclcpp::QoS>` form of the same metadata.
  rclcpp::QoS to_offered_qos() const;
};

/// One topic the recorder subscribes to and writes into the bag.
struct RecordTopic
{
  std::string topic;
  std::string type;
  QosSpec qos;
  BufferingStrategy strategy = BufferingStrategy::NoBuffer;
};

/// Everything the recorder needs from a contract file.
struct Contract
{
  std::string robot_type;
  int fps = 30;
  double max_duration_s = 30.0;
  /// `recording.storage`, empty when unset.
  std::string storage;
  /// Topics in the same order the Python node builds them: observations,
  /// actions, extended (rewards/signals/info/complementary_data), tasks,
  /// adjunct. Order is preserved because it determines bag topic ids.
  std::vector<RecordTopic> topics;
};

/// Load and validate a contract for recording purposes.
/// \throws ContractValidationError on malformed YAML or missing fields.
Contract load_contract(const std::string & path);

}  // namespace rosetta_recorder_cpp

#endif  // ROSETTA_RECORDER_CPP__CONTRACT_HPP_
