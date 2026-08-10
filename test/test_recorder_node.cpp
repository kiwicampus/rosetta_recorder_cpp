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
// Node-level tests: lifecycle, the service and action surface, and one real
// end-to-end recording that produces an MCAP bag on disk.
//
// These run on their own ROS_DOMAIN_ID (set from CMake) so they cannot see, or
// be seen by, a robot running on the default domain.

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <regex>
#include <string>
#include <thread>

#include "lifecycle_msgs/msg/state.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_action/rclcpp_action.hpp"
#include "std_msgs/msg/string.hpp"
#include "std_srvs/srv/trigger.hpp"
#include "yaml-cpp/yaml.h"

#include "rosetta_recorder_cpp/episode_recorder_node.hpp"

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using rosetta_recorder_cpp::EpisodeRecorderNode;
using RecordEpisode = rosetta_interfaces::action::RecordEpisode;
using StartRecording = rosetta_interfaces::srv::StartRecording;
using Trigger = std_srvs::srv::Trigger;

namespace
{

const char * kContractBody = R"(
robot_type: test_bot
fps: 30
observations:
  - key: observation.state.a
    topic: /test_erc/a
    type: std_msgs/msg/String
    qos: {reliability: reliable, history: keep_last, depth: 10}
  - key: observation.state.b
    topic: /test_erc/b
    type: std_msgs/msg/String
    qos: {reliability: reliable, history: keep_last, depth: 10}
adjunct:
  - topic: /test_erc/latched
    type: std_msgs/msg/String
    qos: {reliability: reliable, durability: transient_local, history: keep_last, depth: 1}
)";

/// rosetta's original bag name: <epoch_sec>_<nsec>, zero padded to 10 and 9.
bool is_epoch_name(const std::string & name)
{
  static const std::regex re(R"(^\d{10}_\d{9}$)");
  return std::regex_match(name, re);
}

/// The fork's bag name: YYYYMMDD-HHMMSS-mmm in local time.
bool is_datetime_name(const std::string & name)
{
  static const std::regex re(R"(^\d{8}-\d{6}-\d{3}$)");
  return std::regex_match(name, re);
}

/// Poll a predicate until it holds or the deadline passes.
template<typename Fn>
bool wait_for(Fn && fn, std::chrono::milliseconds timeout, std::chrono::milliseconds step = 20ms)
{
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (fn()) {
      return true;
    }
    std::this_thread::sleep_for(step);
  }
  return fn();
}

}  // namespace

class RecorderNodeTest : public ::testing::Test
{
public:
  static void SetUpTestSuite()
  {
    rclcpp::init(0, nullptr);
  }

  static void TearDownTestSuite()
  {
    rclcpp::shutdown();
  }

protected:
  void SetUp() override
  {
    static int counter = 0;
    const std::string tag = std::to_string(::getpid()) + "_" + std::to_string(counter++);
    root_ = fs::temp_directory_path() / ("erc_test_" + tag);
    fs::create_directories(root_);
    bag_base_ = root_ / "bags";
    contract_path_ = root_ / "contract.yaml";

    std::ofstream out(contract_path_);
    out << kContractBody;
    out.close();
  }

  void TearDown() override
  {
    stop_spin();
    node_.reset();
    helper_.reset();
    std::error_code ec;
    fs::remove_all(root_, ec);
  }

  /// Build the recorder. Extra overrides let a test change one parameter.
  void make_node(const std::vector<rclcpp::Parameter> & extra = {})
  {
    rclcpp::NodeOptions opts;
    std::vector<rclcpp::Parameter> params{
      rclcpp::Parameter("contract_path", contract_path_.string()),
      rclcpp::Parameter("bag_base_dir", bag_base_.string()),
      rclcpp::Parameter("storage_id", std::string("mcap")),
      rclcpp::Parameter("default_max_duration", 60.0),
      // Fast ticks: the tick is what observes a stop request and closes the bag.
      rclcpp::Parameter("feedback_rate_hz", 20.0),
    };
    for (const auto & p : extra) {
      params.push_back(p);
    }
    opts.parameter_overrides(params);
    node_ = std::make_shared<EpisodeRecorderNode>(opts);
  }

  /// A second node for clients and publishers, so nothing under test is
  /// short-circuited by living in the same node.
  void make_helper()
  {
    helper_ = std::make_shared<rclcpp::Node>("erc_test_helper");
  }

  void start_spin()
  {
    exec_ = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
    exec_->add_node(node_->get_node_base_interface());
    spin_thread_ = std::thread([this] {exec_->spin();});

    if (helper_) {
      helper_exec_ = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
      helper_exec_->add_node(helper_);
      helper_thread_ = std::thread([this] {helper_exec_->spin();});
    }
  }

  void stop_spin()
  {
    if (exec_) {
      exec_->cancel();
    }
    if (helper_exec_) {
      helper_exec_->cancel();
    }
    if (spin_thread_.joinable()) {
      spin_thread_.join();
    }
    if (helper_thread_.joinable()) {
      helper_thread_.join();
    }
    exec_.reset();
    helper_exec_.reset();
  }

  /// Configure + activate before spinning: the transitions run synchronously in
  /// the caller, so doing them first keeps the executor out of the way.
  void configure_and_activate()
  {
    ASSERT_EQ(
      node_->configure().id(),
      lifecycle_msgs::msg::State::PRIMARY_STATE_INACTIVE);
    ASSERT_EQ(
      node_->activate().id(),
      lifecycle_msgs::msg::State::PRIMARY_STATE_ACTIVE);
  }

  /// Run one episode and return the name of the bag directory it created, or
  /// "" if none appeared. Leaves the recorder stopped.
  std::string record_one_bag_name()
  {
    auto req = std::make_shared<StartRecording::Request>();
    req->prompt = "naming";
    auto started = call<StartRecording>("/episode_recorder/start_recording", req);
    if (!started || !started->accepted) {
      return {};
    }

    std::string name;
    wait_for(
      [&] {
        if (!fs::exists(bag_base_)) {
          return false;
        }
        for (const auto & e : fs::directory_iterator(bag_base_)) {
          name = e.path().filename().string();
          return true;
        }
        return false;
      }, 5s);

    call<Trigger>("/episode_recorder/cancel_recording", std::make_shared<Trigger::Request>());
    return name;
  }

  template<typename SrvT>
  typename SrvT::Response::SharedPtr call(
    const std::string & name, typename SrvT::Request::SharedPtr req,
    std::chrono::milliseconds timeout = 5s)
  {
    auto client = helper_->create_client<SrvT>(name);
    if (!client->wait_for_service(timeout)) {
      return nullptr;
    }
    auto future = client->async_send_request(req);
    if (future.wait_for(timeout) != std::future_status::ready) {
      return nullptr;
    }
    return future.get();
  }

  fs::path root_;
  fs::path bag_base_;
  fs::path contract_path_;
  std::shared_ptr<EpisodeRecorderNode> node_;
  std::shared_ptr<rclcpp::Node> helper_;
  std::shared_ptr<rclcpp::executors::SingleThreadedExecutor> exec_;
  std::shared_ptr<rclcpp::executors::SingleThreadedExecutor> helper_exec_;
  std::thread spin_thread_;
  std::thread helper_thread_;
};

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

TEST_F(RecorderNodeTest, ConfigureFailsWithoutContractPath)
{
  rclcpp::NodeOptions opts;
  opts.parameter_overrides({rclcpp::Parameter("bag_base_dir", bag_base_.string())});
  node_ = std::make_shared<EpisodeRecorderNode>(opts);

  EXPECT_EQ(
    node_->configure().id(),
    lifecycle_msgs::msg::State::PRIMARY_STATE_UNCONFIGURED);
}

TEST_F(RecorderNodeTest, ConfigureFailsOnAnUnreadableContract)
{
  std::ofstream(contract_path_) << "this: [is: not: a: contract\n";
  make_node();
  EXPECT_EQ(
    node_->configure().id(),
    lifecycle_msgs::msg::State::PRIMARY_STATE_UNCONFIGURED);
}

TEST_F(RecorderNodeTest, ConfigureSubscribesToEveryContractTopic)
{
  make_node();
  configure_and_activate();

  EXPECT_EQ(node_->count_subscribers("/test_erc/a"), 1u);
  EXPECT_EQ(node_->count_subscribers("/test_erc/b"), 1u);
  EXPECT_EQ(node_->count_subscribers("/test_erc/latched"), 1u);
}

TEST_F(RecorderNodeTest, DuplicateContractTopicsProduceOneSubscription)
{
  // The Python node subscribes twice and writes every message into the bag
  // twice; this port drops the duplicate.
  std::ofstream(contract_path_) << R"(
robot_type: test_bot
observations:
  - key: observation.state.a
    topic: /test_erc/dup
    type: std_msgs/msg/String
  - key: observation.state.b
    topic: /test_erc/dup
    type: std_msgs/msg/String
)";
  make_node();
  configure_and_activate();
  EXPECT_EQ(node_->count_subscribers("/test_erc/dup"), 1u);
}

TEST_F(RecorderNodeTest, CleanupReleasesSubscriptionsAndReconfigureWorks)
{
  make_node();
  configure_and_activate();
  ASSERT_EQ(
    node_->deactivate().id(),
    lifecycle_msgs::msg::State::PRIMARY_STATE_INACTIVE);
  ASSERT_EQ(
    node_->cleanup().id(),
    lifecycle_msgs::msg::State::PRIMARY_STATE_UNCONFIGURED);
  EXPECT_EQ(node_->count_subscribers("/test_erc/a"), 0u);

  // A cleanup/configure cycle must leave the node usable — cleanup also has to
  // have joined the writer thread rather than leaving it parked.
  ASSERT_EQ(
    node_->configure().id(),
    lifecycle_msgs::msg::State::PRIMARY_STATE_INACTIVE);
  EXPECT_EQ(node_->count_subscribers("/test_erc/a"), 1u);
}

// ---------------------------------------------------------------------------
// Service surface
// ---------------------------------------------------------------------------

TEST_F(RecorderNodeTest, StartIsRejectedWhileInactive)
{
  make_node();
  make_helper();
  ASSERT_EQ(
    node_->configure().id(),
    lifecycle_msgs::msg::State::PRIMARY_STATE_INACTIVE);
  start_spin();

  auto res = call<StartRecording>(
    "/episode_recorder/start_recording", std::make_shared<StartRecording::Request>());
  ASSERT_NE(res, nullptr);
  EXPECT_FALSE(res->accepted);
  EXPECT_EQ(res->message, "Node not active");
}

TEST_F(RecorderNodeTest, CancelWithNothingRunningReports)
{
  make_node();
  make_helper();
  configure_and_activate();
  start_spin();

  auto res = call<Trigger>(
    "/episode_recorder/cancel_recording", std::make_shared<Trigger::Request>());
  ASSERT_NE(res, nullptr);
  EXPECT_FALSE(res->success);
  EXPECT_EQ(res->message, "No active recording");
}

TEST_F(RecorderNodeTest, DeleteLastBagWithNoBagReports)
{
  make_node();
  make_helper();
  configure_and_activate();
  start_spin();

  auto res = call<Trigger>(
    "/episode_recorder/delete_last_bag", std::make_shared<Trigger::Request>());
  ASSERT_NE(res, nullptr);
  EXPECT_FALSE(res->success);
  EXPECT_EQ(res->message, "No bag to delete");
}

TEST_F(RecorderNodeTest, SecondStartIsRejectedWhileRecording)
{
  make_node();
  make_helper();
  configure_and_activate();
  start_spin();

  auto req = std::make_shared<StartRecording::Request>();
  req->prompt = "first";
  auto first = call<StartRecording>("/episode_recorder/start_recording", req);
  ASSERT_NE(first, nullptr);
  ASSERT_TRUE(first->accepted);

  auto second = call<StartRecording>("/episode_recorder/start_recording", req);
  ASSERT_NE(second, nullptr);
  EXPECT_FALSE(second->accepted);
  EXPECT_EQ(second->message, "Already recording");

  call<Trigger>("/episode_recorder/cancel_recording", std::make_shared<Trigger::Request>());
}

// ---------------------------------------------------------------------------
// End to end
// ---------------------------------------------------------------------------

TEST_F(RecorderNodeTest, ServiceRecordingWritesABagWithThePromptInMetadata)
{
  make_node();
  make_helper();
  configure_and_activate();
  start_spin();

  auto pub = helper_->create_publisher<std_msgs::msg::String>("/test_erc/a", rclcpp::QoS(10));
  ASSERT_TRUE(
    wait_for([&] {return pub->get_subscription_count() > 0;}, 5s)) <<
    "recorder never connected to the publisher";

  auto req = std::make_shared<StartRecording::Request>();
  req->prompt = "pick up the cube";
  auto started = call<StartRecording>("/episode_recorder/start_recording", req);
  ASSERT_NE(started, nullptr);
  ASSERT_TRUE(started->accepted) << started->message;

  for (int i = 0; i < 20; ++i) {
    std_msgs::msg::String msg;
    msg.data = "sample " + std::to_string(i);
    pub->publish(msg);
    std::this_thread::sleep_for(10ms);
  }
  std::this_thread::sleep_for(300ms);

  auto stopped = call<Trigger>(
    "/episode_recorder/cancel_recording", std::make_shared<Trigger::Request>());
  ASSERT_NE(stopped, nullptr);
  EXPECT_TRUE(stopped->success);

  // The bag is finalized by the tick that observes the stop.
  ASSERT_TRUE(
    wait_for(
      [&] {
        if (!fs::exists(bag_base_)) {return false;}
        for (const auto & e : fs::directory_iterator(bag_base_)) {
          if (fs::exists(e.path() / "metadata.yaml")) {return true;}
        }
        return false;
      }, 10s)) << "no finalized bag appeared under " << bag_base_;

  fs::path bag_dir;
  for (const auto & e : fs::directory_iterator(bag_base_)) {
    bag_dir = e.path();
  }
  ASSERT_FALSE(bag_dir.empty());

  // Bag directory name: rosetta's <epoch_sec>_<nsec>, the default style.
  const std::string name = bag_dir.filename().string();
  EXPECT_TRUE(is_epoch_name(name)) << name;

  const YAML::Node meta = YAML::LoadFile((bag_dir / "metadata.yaml").string());
  const YAML::Node info = meta["rosbag2_bagfile_information"];
  ASSERT_TRUE(info) << "metadata.yaml has no rosbag2_bagfile_information";
  ASSERT_TRUE(info["custom_data"]) << "prompt was not injected";
  EXPECT_EQ(
    info["custom_data"]["lerobot.operator_prompt"].as<std::string>(),
    "pick up the cube");

  // Every contract topic is registered, whether or not it carried traffic, and
  // the messages we published are in there.
  ASSERT_TRUE(info["topics_with_message_count"]);
  EXPECT_EQ(info["topics_with_message_count"].size(), 3u);
  EXPECT_GT(info["message_count"].as<int>(), 0);

  // ...and the QoS metadata is in whichever form this distro's rosbag2 writes.
  const YAML::Node first_topic = info["topics_with_message_count"][0]["topic_metadata"];
  const YAML::Node offered_qos = first_topic["offered_qos_profiles"];
  ASSERT_TRUE(offered_qos);
#ifdef ROSETTA_RECORDER_QOS_OBJECT_METADATA
  // Jazzy and newer (bag version >= 9): a sequence of QoS mappings, one per
  // offered profile. Same fields the Humble string spells out, as real keys.
  ASSERT_TRUE(offered_qos.IsSequence()) << YAML::Dump(offered_qos);
  ASSERT_EQ(offered_qos.size(), 1u) << YAML::Dump(offered_qos);
  const YAML::Node profile = offered_qos[0];
  EXPECT_TRUE(profile["history"]) << YAML::Dump(profile);
  EXPECT_TRUE(profile["avoid_ros_namespace_conventions"]) << YAML::Dump(profile);
#else
  // Humble: a single YAML string, byte-compatible with the Python recorder's bags.
  const std::string offered = offered_qos.as<std::string>();
  EXPECT_NE(offered.find("history:"), std::string::npos) << offered;
  EXPECT_NE(offered.find("avoid_ros_namespace_conventions:"), std::string::npos) << offered;
#endif
}

// ---------------------------------------------------------------------------
// Bag directory naming
// ---------------------------------------------------------------------------

TEST_F(RecorderNodeTest, DefaultsToRosettasEpochBagName)
{
  // No override: the default is rosetta's own convention, so the node drops in
  // for the upstream Python recorder without a naming change.
  make_node();
  make_helper();
  configure_and_activate();
  start_spin();

  const std::string name = record_one_bag_name();
  ASSERT_FALSE(name.empty()) << "no bag directory appeared under " << bag_base_;
  EXPECT_TRUE(is_epoch_name(name)) << name;
  EXPECT_FALSE(is_datetime_name(name)) << name;
}

TEST_F(RecorderNodeTest, EpochBagNameSecondsMatchTheWallClock)
{
  // The epoch seconds field is a real timestamp, not just the right shape.
  make_node({rclcpp::Parameter("bag_name_style", std::string("epoch"))});
  make_helper();
  configure_and_activate();
  start_spin();

  const auto before = std::chrono::duration_cast<std::chrono::seconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
  const std::string name = record_one_bag_name();
  const auto after = std::chrono::duration_cast<std::chrono::seconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();

  ASSERT_TRUE(is_epoch_name(name)) << name;
  const int64_t secs = std::stoll(name.substr(0, 10));
  EXPECT_GE(secs, before) << name;
  EXPECT_LE(secs, after) << name;
}

TEST_F(RecorderNodeTest, DateTimeBagNameStyleUsesTheForkConvention)
{
  make_node({rclcpp::Parameter("bag_name_style", std::string("datetime"))});
  make_helper();
  configure_and_activate();
  start_spin();

  const std::string name = record_one_bag_name();
  ASSERT_FALSE(name.empty()) << "no bag directory appeared under " << bag_base_;
  EXPECT_TRUE(is_datetime_name(name)) << name;
  EXPECT_FALSE(is_epoch_name(name)) << name;
}

TEST_F(RecorderNodeTest, ConfigureFailsOnAnUnknownBagNameStyle)
{
  // A typo must not silently fall back to a convention the operator did not ask
  // for: the bag name is what downstream keys off.
  make_node({rclcpp::Parameter("bag_name_style", std::string("iso8601"))});
  EXPECT_EQ(
    node_->configure().id(),
    lifecycle_msgs::msg::State::PRIMARY_STATE_UNCONFIGURED);
}

TEST_F(RecorderNodeTest, ResubscribeOnStartCapturesTheRedeliveredLatchedSample)
{
  // The regression this exists for: the settle wait after recreating the
  // subscription used to be a sleep on the executor thread, so the re-delivered
  // sample could not be received during it and the buffer was always empty.
  // Flushing a message here is only possible if the executor kept spinning.
  std::ofstream(contract_path_) << R"(
robot_type: test_bot
adjunct:
  - topic: /test_erc/latched_rs
    type: std_msgs/msg/String
    buffering_strategy: resubscribe_on_start
    qos: {reliability: reliable, durability: transient_local, history: keep_last, depth: 1}
)";
  make_node();
  make_helper();
  configure_and_activate();
  start_spin();

  auto pub = helper_->create_publisher<std_msgs::msg::String>(
    "/test_erc/latched_rs", rclcpp::QoS(1).reliable().transient_local());
  ASSERT_TRUE(wait_for([&] {return pub->get_subscription_count() > 0;}, 5s));

  std_msgs::msg::String latched;
  latched.data = "latched payload";
  pub->publish(latched);
  std::this_thread::sleep_for(300ms);

  auto req = std::make_shared<StartRecording::Request>();
  req->prompt = "resubscribe";
  auto started = call<StartRecording>("/episode_recorder/start_recording", req);
  ASSERT_NE(started, nullptr);
  ASSERT_TRUE(started->accepted) << started->message;

  // The start is deferred by the settle delay, so give it room before stopping.
  std::this_thread::sleep_for(600ms);
  call<Trigger>("/episode_recorder/cancel_recording", std::make_shared<Trigger::Request>());

  ASSERT_TRUE(
    wait_for(
      [&] {
        if (!fs::exists(bag_base_)) {return false;}
        for (const auto & e : fs::directory_iterator(bag_base_)) {
          if (fs::exists(e.path() / "metadata.yaml")) {return true;}
        }
        return false;
      }, 10s)) << "no finalized bag appeared";

  fs::path bag_dir;
  for (const auto & e : fs::directory_iterator(bag_base_)) {
    bag_dir = e.path();
  }
  const YAML::Node info =
    YAML::LoadFile((bag_dir / "metadata.yaml").string())["rosbag2_bagfile_information"];
  ASSERT_TRUE(info["topics_with_message_count"]);
  ASSERT_EQ(info["topics_with_message_count"].size(), 1u);
  EXPECT_GE(info["topics_with_message_count"][0]["message_count"].as<int>(), 1)
    << "the re-delivered latched sample was not flushed into the bag";
}

TEST_F(RecorderNodeTest, DeleteLastBagRemovesTheBagThenReportsNoBag)
{
  make_node();
  make_helper();
  configure_and_activate();
  start_spin();

  auto req = std::make_shared<StartRecording::Request>();
  req->prompt = "throwaway";
  ASSERT_TRUE(call<StartRecording>("/episode_recorder/start_recording", req)->accepted);
  std::this_thread::sleep_for(200ms);
  call<Trigger>("/episode_recorder/cancel_recording", std::make_shared<Trigger::Request>());

  ASSERT_TRUE(
    wait_for(
      [&] {
        return fs::exists(bag_base_) &&
        fs::directory_iterator(bag_base_) != fs::directory_iterator{};
      }, 10s));
  // Give the tick time to finish metadata before deleting.
  std::this_thread::sleep_for(500ms);

  auto first = call<Trigger>(
    "/episode_recorder/delete_last_bag", std::make_shared<Trigger::Request>());
  ASSERT_NE(first, nullptr);
  EXPECT_TRUE(first->success) << first->message;

  auto second = call<Trigger>(
    "/episode_recorder/delete_last_bag", std::make_shared<Trigger::Request>());
  ASSERT_NE(second, nullptr);
  EXPECT_FALSE(second->success);
  EXPECT_EQ(second->message, "No bag to delete");
}

// ---------------------------------------------------------------------------
// Action, and the terminal state ~/cancel_recording must produce
// ---------------------------------------------------------------------------

TEST_F(RecorderNodeTest, CancelRecordingServiceTerminatesTheGoalAsCanceled)
{
  // This is the behaviour data_capture keys off: a SUCCEEDED result means "hit
  // max duration, unintended stop" and the bag gets discarded, so a stop
  // requested through the service has to come back CANCELED.
  make_node();
  make_helper();
  configure_and_activate();
  start_spin();

  auto client = rclcpp_action::create_client<RecordEpisode>(helper_, "/record_episode");
  ASSERT_TRUE(client->wait_for_action_server(10s));

  RecordEpisode::Goal goal;
  goal.prompt = "cancel me";

  auto goal_future = client->async_send_goal(goal);
  ASSERT_EQ(goal_future.wait_for(10s), std::future_status::ready);
  auto handle = goal_future.get();
  ASSERT_NE(handle, nullptr) << "goal was rejected";

  auto result_future = client->async_get_result(handle);
  std::this_thread::sleep_for(200ms);

  auto stopped = call<Trigger>(
    "/episode_recorder/cancel_recording", std::make_shared<Trigger::Request>());
  ASSERT_NE(stopped, nullptr);
  EXPECT_TRUE(stopped->success);

  ASSERT_EQ(result_future.wait_for(15s), std::future_status::ready);
  const auto result = result_future.get();
  EXPECT_EQ(result.code, rclcpp_action::ResultCode::CANCELED);
  EXPECT_FALSE(result.result->success);
  EXPECT_FALSE(result.result->bag_path.empty());
}

TEST_F(RecorderNodeTest, ActionCancelTerminatesTheGoalAsCanceled)
{
  make_node();
  make_helper();
  configure_and_activate();
  start_spin();

  auto client = rclcpp_action::create_client<RecordEpisode>(helper_, "/record_episode");
  ASSERT_TRUE(client->wait_for_action_server(10s));

  RecordEpisode::Goal goal;
  goal.prompt = "cancel me too";
  auto goal_future = client->async_send_goal(goal);
  ASSERT_EQ(goal_future.wait_for(10s), std::future_status::ready);
  auto handle = goal_future.get();
  ASSERT_NE(handle, nullptr);

  auto result_future = client->async_get_result(handle);
  std::this_thread::sleep_for(200ms);

  auto cancel_future = client->async_cancel_goal(handle);
  ASSERT_EQ(cancel_future.wait_for(10s), std::future_status::ready);

  ASSERT_EQ(result_future.wait_for(15s), std::future_status::ready);
  const auto result = result_future.get();
  EXPECT_EQ(result.code, rclcpp_action::ResultCode::CANCELED);
  EXPECT_FALSE(result.result->success);
}

TEST_F(RecorderNodeTest, MaxDurationStopTerminatesTheGoalAsSucceeded)
{
  // The mirror of the test above: an unintended max-duration stop must stay
  // SUCCEEDED, because that is how data_capture recognises it.
  make_node({rclcpp::Parameter("default_max_duration", 1.0)});
  make_helper();
  configure_and_activate();
  start_spin();

  auto client = rclcpp_action::create_client<RecordEpisode>(helper_, "/record_episode");
  ASSERT_TRUE(client->wait_for_action_server(10s));

  RecordEpisode::Goal goal;
  goal.prompt = "run to the end";
  auto goal_future = client->async_send_goal(goal);
  ASSERT_EQ(goal_future.wait_for(10s), std::future_status::ready);
  auto handle = goal_future.get();
  ASSERT_NE(handle, nullptr);

  auto result_future = client->async_get_result(handle);
  ASSERT_EQ(result_future.wait_for(20s), std::future_status::ready);
  const auto result = result_future.get();
  EXPECT_EQ(result.code, rclcpp_action::ResultCode::SUCCEEDED);
  EXPECT_TRUE(result.result->success);
  EXPECT_FALSE(result.result->bag_path.empty());
}

TEST_F(RecorderNodeTest, SecondGoalIsRejectedWhileRecording)
{
  make_node();
  make_helper();
  configure_and_activate();
  start_spin();

  auto client = rclcpp_action::create_client<RecordEpisode>(helper_, "/record_episode");
  ASSERT_TRUE(client->wait_for_action_server(10s));

  RecordEpisode::Goal goal;
  goal.prompt = "first";
  auto first_future = client->async_send_goal(goal);
  ASSERT_EQ(first_future.wait_for(10s), std::future_status::ready);
  auto first = first_future.get();
  ASSERT_NE(first, nullptr);

  auto second_future = client->async_send_goal(goal);
  ASSERT_EQ(second_future.wait_for(10s), std::future_status::ready);
  EXPECT_EQ(second_future.get(), nullptr) << "a second goal should have been rejected";

  client->async_cancel_goal(first);
  std::this_thread::sleep_for(500ms);
}
