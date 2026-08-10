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
// C++ port of rosetta's episode_recorder_node.py.
//
// Same external surface: node name `episode_recorder`, a RecordEpisode action,
// ~/start_recording, ~/cancel_recording and ~/delete_last_bag services, and the
// standard lifecycle transitions. Same bag layout and metadata, so downstream
// converters and players need no changes.
//
// Two deliberate departures from the Python implementation, both driven by the
// CPU problem this port exists to solve:
//
//  1. Subscriptions are generic (rclcpp::GenericSubscription): messages stay as
//     CDR from the wire to the bag. The Python node deserialized every message
//     into a Python object and then re-serialized it, purely to throw the
//     object away.
//  2. Nothing blocks the executor. The Python node's action handler slept in a
//     loop on a worker thread; here a wall timer drives feedback and timeout,
//     and disk writes happen on a dedicated writer thread fed by a queue, so a
//     slow flush can never stall message reception.

#ifndef ROSETTA_RECORDER_CPP__EPISODE_RECORDER_NODE_HPP_
#define ROSETTA_RECORDER_CPP__EPISODE_RECORDER_NODE_HPP_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "action_msgs/srv/cancel_goal.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp_action/rclcpp_action.hpp"
#include "rclcpp_lifecycle/lifecycle_node.hpp"
#include "rosbag2_cpp/writer.hpp"
#include "std_srvs/srv/trigger.hpp"

#include "rosetta_interfaces/action/record_episode.hpp"
#include "rosetta_interfaces/srv/start_recording.hpp"

#include "rosetta_recorder_cpp/contract.hpp"

namespace rosetta_recorder_cpp
{

/// Keys used for the prompt in the bag's metadata.yaml.
constexpr const char * kBagMetadataKey = "rosbag2_bagfile_information";
constexpr const char * kBagCustomDataKey = "custom_data";
constexpr const char * kBagPromptKey = "lerobot.operator_prompt";

/// metadata.yaml is written by rosbag2 when the bag closes; retry while we wait
/// for it to appear.
constexpr int kMetadataRetryCount = 10;
constexpr auto kMetadataRetryDelay = std::chrono::milliseconds(100);

/// Cap on a single retained (latched) message we are willing to hold, 4 MiB.
constexpr size_t kMaxBufferBytes = 4u * 1024u * 1024u;

/// Bound on the writer queue. At ~11 MiB/s and 2k msg/s this is several seconds
/// of slack, enough to ride out a disk stall without unbounded memory growth.
constexpr size_t kWriterQueueMaxMessages = 20000;
constexpr size_t kWriterQueueMaxBytes = 512u * 1024u * 1024u;

/// Topics never auto-recorded, matching `ros2 bag record --exclude` convention.
extern const char * const kDefaultBlacklist[2];

/// How long to wait for a publisher to re-deliver its latched sample after a
/// `resubscribe_on_start` subscription is recreated. Same 200 ms the Python node
/// slept for — but spent on a timer, so the executor keeps delivering messages
/// during the wait instead of being blocked by it.
constexpr auto kResubscribeSettleDelay = std::chrono::milliseconds(200);

class EpisodeRecorderNode : public rclcpp_lifecycle::LifecycleNode
{
public:
  using RecordEpisode = rosetta_interfaces::action::RecordEpisode;
  using GoalHandle = rclcpp_action::ServerGoalHandle<RecordEpisode>;
  using StartRecording = rosetta_interfaces::srv::StartRecording;
  using Trigger = std_srvs::srv::Trigger;
  using CallbackReturn = rclcpp_lifecycle::node_interfaces::LifecycleNodeInterface
    ::CallbackReturn;

  explicit EpisodeRecorderNode(const rclcpp::NodeOptions & options = rclcpp::NodeOptions());
  ~EpisodeRecorderNode() override;

  // -------------------- Lifecycle --------------------
  CallbackReturn on_configure(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_activate(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_deactivate(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_cleanup(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_shutdown(const rclcpp_lifecycle::State & state) override;
  CallbackReturn on_error(const rclcpp_lifecycle::State & state) override;

private:
  /// One serialized message on its way to disk.
  struct QueuedMessage
  {
    std::shared_ptr<rclcpp::SerializedMessage> data;
    int64_t timestamp_ns;
    size_t topic_index;
  };

  /// A retained latched message held while not recording.
  struct BufferedMessage
  {
    std::shared_ptr<rclcpp::SerializedMessage> data;
    int64_t timestamp_ns;
  };

  /// Per-topic runtime state.
  struct TopicState
  {
    RecordTopic spec;
    std::shared_ptr<rclcpp::GenericSubscription> sub;
    /// Retained latched messages, capped at spec.qos.depth.
    std::deque<BufferedMessage> buffer;
    std::atomic<uint64_t> written{0};
    /// True for topics discovered by record_all rather than named in the
    /// contract; these are torn down when the bag closes.
    bool discovered = false;
  };

  // -------------------- Setup --------------------
  void declare_parameters();
  /// Build the topic list from the contract, dropping duplicates.
  void build_topic_list();
  void create_subscription_for(size_t index);
  void destroy_discovered_subscriptions();
  /// Topics on the graph that are not in the contract and not excluded.
  void discover_topics();

  // -------------------- Message path --------------------
  void on_message(size_t topic_index, std::shared_ptr<rclcpp::SerializedMessage> msg);

  // -------------------- Recording --------------------
  /// Common entry point for both the action and the service.
  bool start_recording(const std::string & prompt, std::string & error_out);
  /// Recreate `resubscribe_on_start` subscriptions (and, under record_all,
  /// discover topics). Returns how many subscriptions were recreated; a nonzero
  /// count means the episode start is deferred by kResubscribeSettleDelay.
  size_t prepare_resubscribes();
  /// Second half of a deferred start: report what the recreated subscriptions
  /// received, open the bag, and go live.
  void complete_start();
  /// Drop a start that is still waiting on its settle timer.
  void cancel_pending_start(const std::string & reason);
  /// Open the bag and arm the feedback timer. \throws on storage failure.
  void begin_episode();
  /// Timer tick: publishes feedback, enforces max duration, honors cancel.
  void on_recording_tick();
  /// Close the bag, stamp metadata, settle the action goal.
  void finish_recording(const std::string & reason, bool canceled);
  /// Ask our own action server to cancel the live goal, so it terminates as
  /// CANCELED rather than SUCCEEDED. Returns false when there is nothing to
  /// cancel or the cancel service is unavailable, in which case the caller must
  /// fall back to stop_requested_.
  bool request_action_cancel();

  std::filesystem::path create_bag_dir();
  void open_writer(const std::filesystem::path & bag_dir);
  void close_writer();
  /// \throws std::runtime_error when the prompt cannot be persisted.
  void write_metadata(const std::filesystem::path & bag_dir, const std::string & prompt);

  // -------------------- Writer thread --------------------
  void writer_thread_main();
  void stop_writer_thread();

  // -------------------- Action / service callbacks --------------------
  rclcpp_action::GoalResponse handle_goal(
    const rclcpp_action::GoalUUID & uuid, std::shared_ptr<const RecordEpisode::Goal> goal);
  rclcpp_action::CancelResponse handle_cancel(std::shared_ptr<GoalHandle> handle);
  void handle_accepted(std::shared_ptr<GoalHandle> handle);

  void on_start_service(
    const std::shared_ptr<StartRecording::Request> req,
    std::shared_ptr<StartRecording::Response> res);
  void on_cancel_service(
    const std::shared_ptr<Trigger::Request> req, std::shared_ptr<Trigger::Response> res);
  void on_delete_last_bag_service(
    const std::shared_ptr<Trigger::Request> req, std::shared_ptr<Trigger::Response> res);

  /// How a new bag directory is named. Selected by the `bag_name_style`
  /// parameter; see create_bag_dir().
  enum class BagNameStyle
  {
    /// rosetta's original `<epoch_sec>_<nsec>`, zero padded to 10 and 9 digits.
    kEpoch,
    /// `YYYYMMDD-HHMMSS-mmm` in local time.
    kDateTime,
  };

  /// \throws std::invalid_argument when the name is not a known style.
  static BagNameStyle parse_bag_name_style(const std::string & name);

  // -------------------- Configuration --------------------
  std::optional<Contract> contract_;
  std::filesystem::path bag_base_;
  std::string storage_id_;
  double default_max_duration_{300.0};
  double feedback_rate_hz_{2.0};
  bool record_all_{false};
  BagNameStyle bag_name_style_{BagNameStyle::kEpoch};
  std::optional<std::regex> exclude_regex_;

  // -------------------- Topics --------------------
  /// Stable indices; subscription callbacks capture an index into this vector,
  /// so it must not be reallocated while subscriptions are live. Elements are
  /// held by pointer for that reason.
  std::vector<std::unique_ptr<TopicState>> topics_;
  std::unordered_map<std::string, size_t> topic_index_;
  /// Guards topics_/topic_index_ against concurrent access between the
  /// executor thread and the writer thread.
  std::mutex topics_mutex_;

  // -------------------- Recording state --------------------
  /// Read on the hot path; set true only after the writer is ready.
  std::atomic<bool> is_recording_{false};
  std::atomic<bool> accepting_goals_{false};
  std::atomic<uint64_t> messages_written_{0};
  std::atomic<uint64_t> messages_dropped_{0};
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> write_failed_{false};
  std::string write_error_;
  /// Why the episode is stopping, when it was not a timeout or action cancel.
  std::string stop_reason_;
  /// Guards write_error_ and stop_reason_.
  std::mutex write_error_mutex_;

  std::filesystem::path current_bag_dir_;
  std::filesystem::path last_bag_dir_;
  std::string current_prompt_;
  rclcpp::Time recording_start_;
  double current_max_duration_{300.0};

  std::shared_ptr<GoalHandle> goal_handle_;
  /// Serializes start/finish against the service and action entry points.
  std::mutex lifecycle_mutex_;

  /// True between accepting a start and actually going live, while the
  /// resubscribe settle delay runs. A second start must be refused during it.
  std::atomic<bool> start_pending_{false};
  rclcpp::TimerBase::SharedPtr pending_start_timer_;
  /// Indices of the subscriptions recreated for this start, reported once the
  /// settle delay has elapsed.
  std::vector<size_t> pending_resubscribed_;

  // -------------------- Writer --------------------
  std::unique_ptr<rosbag2_cpp::Writer> writer_;
  std::mutex writer_mutex_;

  std::deque<QueuedMessage> queue_;
  size_t queue_bytes_{0};
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::thread writer_thread_;
  std::atomic<bool> writer_thread_running_{false};
  /// Set when the producer side is done and the thread should drain and exit
  /// the current bag.
  bool flush_requested_{false};
  bool flush_done_{false};
  std::condition_variable flush_cv_;

  // -------------------- ROS interfaces --------------------
  rclcpp_action::Server<RecordEpisode>::SharedPtr action_server_;
  /// Client onto our own action server's cancel service. The only way to move a
  /// goal into CANCELING — and therefore to terminate it as CANCELED — is
  /// through that service, so ~/cancel_recording goes via this rather than
  /// silently downgrading the episode to SUCCEEDED.
  rclcpp::Client<action_msgs::srv::CancelGoal>::SharedPtr action_cancel_client_;
  rclcpp::Service<StartRecording>::SharedPtr start_service_;
  rclcpp::Service<Trigger>::SharedPtr cancel_service_;
  rclcpp::Service<Trigger>::SharedPtr delete_last_bag_service_;
  rclcpp::TimerBase::SharedPtr recording_timer_;
  rclcpp::CallbackGroup::SharedPtr services_cbg_;
};

}  // namespace rosetta_recorder_cpp

#endif  // ROSETTA_RECORDER_CPP__EPISODE_RECORDER_NODE_HPP_
