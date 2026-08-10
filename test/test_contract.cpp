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
// Contract loading and QoS serialization.
//
// The expected `offered_qos_profiles` strings here are the ones the Python
// recorder emits (rosetta/episode_recorder_node.py::_serialize_offered_qos on
// Humble). They are asserted byte-for-byte: a bag whose metadata drifts from
// the Python node's is a bag the existing players and converters may read
// differently, which is the whole risk this port is trying not to take.

#include <gtest/gtest.h>

#include <unistd.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

#include "rosetta_recorder_cpp/contract.hpp"

namespace fs = std::filesystem;
using rosetta_recorder_cpp::BufferingStrategy;
using rosetta_recorder_cpp::ContractValidationError;
using rosetta_recorder_cpp::QosSpec;
using rosetta_recorder_cpp::load_contract;

namespace
{

/// A contract YAML on disk, removed when the test finishes.
class TempContract
{
public:
  explicit TempContract(const std::string & body)
  {
    static int counter = 0;
    path_ = fs::temp_directory_path() /
      ("erc_contract_" + std::to_string(::getpid()) + "_" + std::to_string(counter++) + ".yaml");
    std::ofstream out(path_);
    out << body;
    out.close();
  }
  ~TempContract()
  {
    std::error_code ec;
    fs::remove(path_, ec);
  }
  const std::string path() const {return path_.string();}

private:
  fs::path path_;
};

/// Every section that maps to a recorded topic, in contract order.
const char * kFullContract = R"(
robot_type: test_bot
fps: 30
observations:
  - key: observation.state.arm
    topic: /arm/joint_states
    type: sensor_msgs/msg/JointState
    qos: {reliability: best_effort, history: keep_last, depth: 50}
  - key: observation.images.cam
    topic: /cam/compressed
    type: sensor_msgs/msg/CompressedImage
    qos: {reliability: best_effort, depth: 10}
actions:
  - key: action
    publish:
      topic: /arm/command
      type: sensor_msgs/msg/JointState
      qos: {reliability: reliable, depth: 5}
rewards:
  - key: next.reward
    topic: /reward
    type: std_msgs/msg/Float32
signals:
  - key: signal.a
    topic: /signal
    type: std_msgs/msg/Bool
info:
  - key: info.a
    topic: /info
    type: std_msgs/msg/String
complementary_data:
  - key: comp.a
    topic: /comp
    type: std_msgs/msg/String
tasks:
  - topic: /task
    type: std_msgs/msg/String
adjunct:
  - topic: /tf_static
    type: tf2_msgs/msg/TFMessage
    qos: {reliability: reliable, durability: transient_local, history: keep_last, depth: 100}
)";

}  // namespace

// ---------------------------------------------------------------------------
// Topic extraction
// ---------------------------------------------------------------------------

TEST(Contract, SectionOrderMatchesPythonBuildTopicList)
{
  // Order fixes the bag's topic ids, so it is part of the contract with
  // downstream consumers: observations, actions, extended (rewards, signals,
  // info, complementary_data), tasks, adjunct.
  TempContract f(kFullContract);
  const auto c = load_contract(f.path());

  ASSERT_EQ(c.topics.size(), 9u);
  EXPECT_EQ(c.topics[0].topic, "/arm/joint_states");
  EXPECT_EQ(c.topics[1].topic, "/cam/compressed");
  EXPECT_EQ(c.topics[2].topic, "/arm/command");
  EXPECT_EQ(c.topics[3].topic, "/reward");
  EXPECT_EQ(c.topics[4].topic, "/signal");
  EXPECT_EQ(c.topics[5].topic, "/info");
  EXPECT_EQ(c.topics[6].topic, "/comp");
  EXPECT_EQ(c.topics[7].topic, "/task");
  EXPECT_EQ(c.topics[8].topic, "/tf_static");

  EXPECT_EQ(c.robot_type, "test_bot");
  EXPECT_EQ(c.fps, 30);
}

TEST(Contract, ActionTopicComesFromPublishBlock)
{
  TempContract f(kFullContract);
  const auto c = load_contract(f.path());
  EXPECT_EQ(c.topics[2].topic, "/arm/command");
  EXPECT_EQ(c.topics[2].type, "sensor_msgs/msg/JointState");
  EXPECT_EQ(c.topics[2].qos.depth, 5);
  EXPECT_TRUE(c.topics[2].qos.reliable);
}

TEST(Contract, AdjunctAcceptsSingleMappingNotOnlyASequence)
{
  TempContract f(R"(
robot_type: test_bot
adjunct:
  topic: /solo
  type: std_msgs/msg/String
)");
  const auto c = load_contract(f.path());
  ASSERT_EQ(c.topics.size(), 1u);
  EXPECT_EQ(c.topics[0].topic, "/solo");
}

// ---------------------------------------------------------------------------
// QoS parsing
// ---------------------------------------------------------------------------

TEST(Contract, QosFieldsParse)
{
  TempContract f(kFullContract);
  const auto c = load_contract(f.path());

  const auto & arm = c.topics[0].qos;
  EXPECT_EQ(arm.depth, 50);
  EXPECT_FALSE(arm.reliable);
  EXPECT_TRUE(arm.keep_last);
  EXPECT_FALSE(arm.transient_local);

  const auto & tf = c.topics[8].qos;
  EXPECT_EQ(tf.depth, 100);
  EXPECT_TRUE(tf.reliable);
  EXPECT_TRUE(tf.transient_local);
}

TEST(Contract, MissingQosBlockGetsPythonBareDepthFallback)
{
  // rosetta falls back to a bare `10`, and extract_qos_numeric_values() reports
  // AUTOMATIC liveliness for an int while reporting SYSTEM_DEFAULT for a real
  // QoSProfile. The distinction is invisible except in bag metadata.
  TempContract f(kFullContract);
  const auto c = load_contract(f.path());

  const auto & no_qos = c.topics[3].qos;  // /reward, no qos: block
  EXPECT_EQ(no_qos.depth, 10);
  EXPECT_TRUE(no_qos.reliable);
  EXPECT_TRUE(no_qos.keep_last);
  EXPECT_FALSE(no_qos.transient_local);
  EXPECT_EQ(no_qos.liveliness, 1);  // AUTOMATIC

  const auto & with_qos = c.topics[0].qos;
  EXPECT_EQ(with_qos.liveliness, 0);  // SYSTEM_DEFAULT
}

TEST(Contract, KeepAllHistoryParses)
{
  TempContract f(R"(
robot_type: test_bot
adjunct:
  - topic: /a
    type: std_msgs/msg/String
    qos: {history: keep_all, depth: 3}
)");
  const auto c = load_contract(f.path());
  EXPECT_FALSE(c.topics[0].qos.keep_last);
  EXPECT_EQ(c.topics[0].qos.depth, 3);
}

TEST(Contract, NonPositiveDepthFallsBackToTen)
{
  TempContract f(R"(
robot_type: test_bot
adjunct:
  - topic: /a
    type: std_msgs/msg/String
    qos: {depth: 0}
)");
  const auto c = load_contract(f.path());
  EXPECT_EQ(c.topics[0].qos.depth, 10);
}

// ---------------------------------------------------------------------------
// Buffering strategy
// ---------------------------------------------------------------------------

TEST(Contract, TransientLocalAdjunctAutoDetectsAccumulate)
{
  TempContract f(kFullContract);
  const auto c = load_contract(f.path());
  EXPECT_EQ(c.topics[8].strategy, BufferingStrategy::Accumulate);
}

TEST(Contract, VolatileAdjunctAutoDetectsNoBuffer)
{
  TempContract f(R"(
robot_type: test_bot
adjunct:
  - topic: /a
    type: std_msgs/msg/String
    qos: {durability: volatile}
)");
  const auto c = load_contract(f.path());
  EXPECT_EQ(c.topics[0].strategy, BufferingStrategy::NoBuffer);
}

TEST(Contract, ObservationsAndActionsAreNeverBuffered)
{
  TempContract f(kFullContract);
  const auto c = load_contract(f.path());
  for (size_t i = 0; i < 8; ++i) {
    EXPECT_EQ(c.topics[i].strategy, BufferingStrategy::NoBuffer) << "topic " << i;
  }
}

TEST(Contract, ExplicitResubscribeOnStartParses)
{
  TempContract f(R"(
robot_type: test_bot
adjunct:
  - topic: /a
    type: std_msgs/msg/String
    buffering_strategy: resubscribe_on_start
    qos: {durability: transient_local, depth: 1}
)");
  const auto c = load_contract(f.path());
  EXPECT_EQ(c.topics[0].strategy, BufferingStrategy::ResubscribeOnStart);
}

TEST(Contract, BufferingOnVolatileTopicIsRejected)
{
  TempContract f(R"(
robot_type: test_bot
adjunct:
  - topic: /a
    type: std_msgs/msg/String
    buffering_strategy: accumulate
)");
  EXPECT_THROW(load_contract(f.path()), ContractValidationError);
}

TEST(Contract, UnknownBufferingStrategyIsRejected)
{
  TempContract f(R"(
robot_type: test_bot
adjunct:
  - topic: /a
    type: std_msgs/msg/String
    buffering_strategy: hold_everything
    qos: {durability: transient_local}
)");
  EXPECT_THROW(load_contract(f.path()), ContractValidationError);
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

TEST(Contract, MissingFileIsRejected)
{
  EXPECT_THROW(load_contract("/nonexistent/contract.yaml"), ContractValidationError);
}

TEST(Contract, MissingRobotTypeIsRejected)
{
  TempContract f("fps: 30\n");
  EXPECT_THROW(load_contract(f.path()), ContractValidationError);
}

TEST(Contract, MissingTopicFieldIsRejected)
{
  TempContract f(R"(
robot_type: test_bot
observations:
  - key: observation.state
    type: std_msgs/msg/String
)");
  EXPECT_THROW(load_contract(f.path()), ContractValidationError);
}

TEST(Contract, MissingTypeFieldIsRejected)
{
  TempContract f(R"(
robot_type: test_bot
observations:
  - key: observation.state
    topic: /a
)");
  EXPECT_THROW(load_contract(f.path()), ContractValidationError);
}

TEST(Contract, ActionWithoutPublishBlockIsRejected)
{
  TempContract f(R"(
robot_type: test_bot
actions:
  - key: action
    topic: /a
    type: std_msgs/msg/String
)");
  EXPECT_THROW(load_contract(f.path()), ContractValidationError);
}

TEST(Contract, NonPositiveFpsIsRejected)
{
  TempContract f("robot_type: test_bot\nfps: 0\n");
  EXPECT_THROW(load_contract(f.path()), ContractValidationError);
}

// ---------------------------------------------------------------------------
// offered_qos_profiles — byte-compatible with the Python recorder
// ---------------------------------------------------------------------------

TEST(QosSpec, OfferedQosYamlMatchesPythonForADefaultProfile)
{
  QosSpec q;  // reliable, keep_last, depth 10, volatile, SYSTEM_DEFAULT liveliness
  const std::string expected =
    "- history: 1\n"
    "  depth: 10\n"
    "  reliability: 1\n"
    "  durability: 2\n"
    "  deadline:\n"
    "    sec: 2147483647\n"
    "    nsec: 4294967295\n"
    "  lifespan:\n"
    "    sec: 2147483647\n"
    "    nsec: 4294967295\n"
    "  liveliness: 0\n"
    "  liveliness_lease_duration:\n"
    "    sec: 2147483647\n"
    "    nsec: 4294967295\n"
    "  avoid_ros_namespace_conventions: false";
  EXPECT_EQ(q.to_offered_qos_yaml(), expected);
}

TEST(QosSpec, OfferedQosYamlMatchesPythonForABestEffortSensorProfile)
{
  // A typical high-rate sensor stream: best-effort, keep-last, deeper queue.
  QosSpec q;
  q.reliable = false;
  q.keep_last = true;
  q.depth = 50;
  const std::string expected =
    "- history: 1\n"
    "  depth: 50\n"
    "  reliability: 2\n"
    "  durability: 2\n"
    "  deadline:\n"
    "    sec: 2147483647\n"
    "    nsec: 4294967295\n"
    "  lifespan:\n"
    "    sec: 2147483647\n"
    "    nsec: 4294967295\n"
    "  liveliness: 0\n"
    "  liveliness_lease_duration:\n"
    "    sec: 2147483647\n"
    "    nsec: 4294967295\n"
    "  avoid_ros_namespace_conventions: false";
  EXPECT_EQ(q.to_offered_qos_yaml(), expected);
}

TEST(QosSpec, OfferedQosYamlEncodesTransientLocalAndKeepAll)
{
  QosSpec q;
  q.transient_local = true;
  q.keep_last = false;
  q.depth = 100;
  const std::string yaml = q.to_offered_qos_yaml();
  EXPECT_NE(yaml.find("history: 2\n"), std::string::npos);      // KEEP_ALL
  EXPECT_NE(yaml.find("durability: 1\n"), std::string::npos);   // TRANSIENT_LOCAL
  EXPECT_NE(yaml.find("depth: 100\n"), std::string::npos);
}

TEST(QosSpec, ToRclcppMapsContractFields)
{
  QosSpec q;
  q.reliable = false;
  q.transient_local = true;
  q.depth = 7;
  const rclcpp::QoS qos = q.to_rclcpp();
  EXPECT_EQ(qos.reliability(), rclcpp::ReliabilityPolicy::BestEffort);
  EXPECT_EQ(qos.durability(), rclcpp::DurabilityPolicy::TransientLocal);
  EXPECT_EQ(qos.history(), rclcpp::HistoryPolicy::KeepLast);
  EXPECT_EQ(qos.depth(), 7u);
}

TEST(QosSpec, ContractTopicsDoNotRequestPublisherOnlyPolicies)
{
  // A contract can only express reliability/history/durability/depth, so the
  // subscription must leave everything else at rclcpp's defaults — exactly what
  // rclpy does with a QoSProfile built by qos_profile_from_dict.
  QosSpec q;
  q.liveliness = 1;  // as if a bare-depth spec
  const rclcpp::QoS qos = q.to_rclcpp();
  EXPECT_EQ(qos.liveliness(), rclcpp::LivelinessPolicy::SystemDefault);
}

TEST(QosSpec, DiscoveredTopicsRequestTheFullPublisherProfile)
{
  QosSpec q;
  q.match_publisher_profile = true;
  q.liveliness = 1;  // AUTOMATIC
  q.deadline_sec = 1;
  q.deadline_nsec = 0;
  const rclcpp::QoS qos = q.to_rclcpp();
  EXPECT_EQ(qos.liveliness(), rclcpp::LivelinessPolicy::Automatic);
  EXPECT_EQ(qos.deadline(), rclcpp::Duration(1, 0));
}

TEST(QosSpec, OfferedQosObjectCarriesEveryFieldRegardlessOfSubscriptionPolicy)
{
  // The Jazzy metadata form must describe the same profile the YAML string does,
  // including fields to_rclcpp() intentionally leaves alone for contract topics.
  QosSpec q;
  q.liveliness = 1;
  const rclcpp::QoS offered = q.to_offered_qos();
  EXPECT_EQ(offered.liveliness(), rclcpp::LivelinessPolicy::Automatic);
}
