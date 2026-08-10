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

#include "rosetta_recorder_cpp/episode_recorder_node.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "rcl_interfaces/msg/parameter_descriptor.hpp"
#include "rmw/types.h"
#include "rosbag2_cpp/writer.hpp"
#include "rosbag2_cpp/writers/sequential_writer.hpp"
#include "rosbag2_storage/serialized_bag_message.hpp"
#include "rosbag2_storage/storage_options.hpp"
#include "rosbag2_storage/topic_metadata.hpp"
#include "yaml-cpp/yaml.h"

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace rosetta_recorder_cpp
{

const char * const kDefaultBlacklist[2] = {"^/rosout$", "^/parameter_events$"};

namespace
{

rcl_interfaces::msg::ParameterDescriptor describe(const std::string & desc, bool read_only = false)
{
  rcl_interfaces::msg::ParameterDescriptor d;
  d.description = desc;
  d.read_only = read_only;
  return d;
}

// rosbag2 split SerializedBagMessage's single time_stamp into recv_timestamp and
// send_timestamp in Jazzy. The recorder only ever knows one time per message —
// when it came off the wire — so both get that value, which is exactly what
// rosbag2 documents for a send time that is not available.
void set_bag_timestamp(rosbag2_storage::SerializedBagMessage & msg, int64_t timestamp_ns)
{
#ifdef ROSETTA_RECORDER_TIMESTAMP_SPLIT
  msg.recv_timestamp = timestamp_ns;
  msg.send_timestamp = timestamp_ns;
#else
  msg.time_stamp = timestamp_ns;
#endif
}

}  // namespace

EpisodeRecorderNode::EpisodeRecorderNode(const rclcpp::NodeOptions & options)
: rclcpp_lifecycle::LifecycleNode("episode_recorder", options)
{
  declare_parameters();
  RCLCPP_INFO(get_logger(), "Node created (unconfigured)");
}

EpisodeRecorderNode::~EpisodeRecorderNode()
{
  stop_requested_.store(true);
  is_recording_.store(false);
  stop_writer_thread();
}

void EpisodeRecorderNode::declare_parameters()
{
  // Defaults must be explicit std::string: a bare literal deduces
  // ParameterT = char[N], which rclcpp does not handle as a string parameter.
  declare_parameter(
    "contract_path", std::string(""), describe("Path to contract YAML file", true));
  declare_parameter(
    "bag_base_dir", std::string("/workspaces/rosetta_ws/datasets/bags"),
    describe("Base directory for bag storage", true));
  declare_parameter(
    "storage_id", std::string("mcap"),
    describe("Bag storage format (mcap, sqlite3)", true));
  declare_parameter(
    "exclude_topics", std::vector<std::string>{""},
    describe(
      "Regex patterns for topics to exclude from auto-recording "
      "(same syntax as ros2 bag record --exclude)", true));
  declare_parameter(
    "default_max_duration", 300.0, describe("Maximum recording duration in seconds"));
  declare_parameter(
    "feedback_rate_hz", 2.0, describe("Rate for publishing action feedback"));
  declare_parameter(
    "record_all", false,
    describe("Record all active topics, not just contract topics", true));
  declare_parameter(
    "bag_name_style", std::string("epoch"),
    describe(
      "How bag directories are named: 'epoch' for rosetta's <epoch_sec>_<nsec>, "
      "or 'datetime' for YYYYMMDD-HHMMSS-mmm in local time", true));
}

EpisodeRecorderNode::BagNameStyle
EpisodeRecorderNode::parse_bag_name_style(const std::string & name)
{
  if (name == "epoch") {
    return BagNameStyle::kEpoch;
  }
  if (name == "datetime") {
    return BagNameStyle::kDateTime;
  }
  throw std::invalid_argument("unknown bag_name_style '" + name + "', expected epoch or datetime");
}

// ===================== Lifecycle =====================

EpisodeRecorderNode::CallbackReturn
EpisodeRecorderNode::on_configure(const rclcpp_lifecycle::State &)
{
  try {
    const auto contract_path = get_parameter("contract_path").as_string();
    if (contract_path.empty()) {
      RCLCPP_ERROR(get_logger(), "contract_path parameter required");
      return CallbackReturn::FAILURE;
    }

    try {
      contract_ = load_contract(contract_path);
    } catch (const std::exception & e) {
      RCLCPP_ERROR(get_logger(), "Failed to load contract: %s", e.what());
      return CallbackReturn::FAILURE;
    }

    bag_base_ = fs::path(get_parameter("bag_base_dir").as_string());
    std::error_code ec;
    fs::create_directories(bag_base_, ec);
    if (ec && !fs::exists(bag_base_)) {
      RCLCPP_ERROR(
        get_logger(), "Cannot create bag_base_dir %s: %s",
        bag_base_.c_str(), ec.message().c_str());
      return CallbackReturn::FAILURE;
    }

    storage_id_ = get_parameter("storage_id").as_string();
    default_max_duration_ = get_parameter("default_max_duration").as_double();
    feedback_rate_hz_ = get_parameter("feedback_rate_hz").as_double();
    if (feedback_rate_hz_ <= 0.0) {
      feedback_rate_hz_ = 2.0;
    }
    record_all_ = get_parameter("record_all").as_bool();

    try {
      bag_name_style_ = parse_bag_name_style(get_parameter("bag_name_style").as_string());
    } catch (const std::invalid_argument & e) {
      RCLCPP_ERROR(get_logger(), "Invalid bag_name_style: %s", e.what());
      return CallbackReturn::FAILURE;
    }

    // Merge default and user exclude patterns into one alternation, the same
    // convention as `ros2 bag record --exclude`.
    std::string joined;
    for (const char * p : kDefaultBlacklist) {
      joined += (joined.empty() ? "" : "|");
      joined += p;
    }
    // Hold the Parameter in a named local: as_string_array() returns a
    // reference into it, and iterating a temporary's reference would read
    // freed memory.
    const rclcpp::Parameter exclude_param = get_parameter("exclude_topics");
    for (const auto & p : exclude_param.as_string_array()) {
      if (!p.empty()) {
        joined += "|";
        joined += p;
      }
    }
    try {
      exclude_regex_ = std::regex(joined, std::regex::ECMAScript);
    } catch (const std::regex_error & e) {
      RCLCPP_ERROR(get_logger(), "Invalid exclude_topics regex: %s", e.what());
      return CallbackReturn::FAILURE;
    }

    build_topic_list();

    // Callbacks are inert until is_recording_ flips, so it is safe to subscribe
    // now and pay discovery cost once rather than per episode.
    for (size_t i = 0; i < topics_.size(); ++i) {
      create_subscription_for(i);
    }

    services_cbg_ = create_callback_group(rclcpp::CallbackGroupType::MutuallyExclusive);

    action_server_ = rclcpp_action::create_server<RecordEpisode>(
      get_node_base_interface(), get_node_clock_interface(),
      get_node_logging_interface(), get_node_waitables_interface(),
      "record_episode",
      std::bind(&EpisodeRecorderNode::handle_goal, this, std::placeholders::_1,
        std::placeholders::_2),
      std::bind(&EpisodeRecorderNode::handle_cancel, this, std::placeholders::_1),
      std::bind(&EpisodeRecorderNode::handle_accepted, this, std::placeholders::_1),
      rcl_action_server_get_default_options(), services_cbg_);

    // A goal can only reach CANCELED by passing through CANCELING, and only the
    // action's own cancel service can put it there. ~/cancel_recording therefore
    // calls that service on ourselves instead of quietly ending the episode as
    // SUCCEEDED, which downstream reads as an unintended max-duration stop.
    // The name is rcl_action's fixed convention: <action>/_action/cancel_goal.
    action_cancel_client_ = create_client<action_msgs::srv::CancelGoal>(
      "record_episode/_action/cancel_goal", rmw_qos_profile_services_default,
      services_cbg_);

    // Foxglove and similar clients cannot reach the hidden _action/* services,
    // so recording is also startable and cancelable over plain services.
    start_service_ = create_service<StartRecording>(
      "~/start_recording",
      std::bind(&EpisodeRecorderNode::on_start_service, this, std::placeholders::_1,
        std::placeholders::_2),
      rmw_qos_profile_services_default, services_cbg_);

    cancel_service_ = create_service<Trigger>(
      "~/cancel_recording",
      std::bind(&EpisodeRecorderNode::on_cancel_service, this, std::placeholders::_1,
        std::placeholders::_2),
      rmw_qos_profile_services_default, services_cbg_);

    delete_last_bag_service_ = create_service<Trigger>(
      "~/delete_last_bag",
      std::bind(&EpisodeRecorderNode::on_delete_last_bag_service, this, std::placeholders::_1,
        std::placeholders::_2),
      rmw_qos_profile_services_default, services_cbg_);

    RCLCPP_INFO(
      get_logger(), "Configured: robot_type=%s, topics=%zu",
      contract_->robot_type.c_str(), topics_.size());
    return CallbackReturn::SUCCESS;
  } catch (const std::exception & e) {
    RCLCPP_ERROR(get_logger(), "Configuration failed: %s", e.what());
    return CallbackReturn::FAILURE;
  }
}

EpisodeRecorderNode::CallbackReturn
EpisodeRecorderNode::on_activate(const rclcpp_lifecycle::State & state)
{
  accepting_goals_.store(true);
  RCLCPP_INFO(get_logger(), "Activated and ready for recording");
  return LifecycleNode::on_activate(state);
}

EpisodeRecorderNode::CallbackReturn
EpisodeRecorderNode::on_deactivate(const rclcpp_lifecycle::State & state)
{
  accepting_goals_.store(false);

  cancel_pending_start("Stopped: node deactivated before recording started");
  if (is_recording_.load()) {
    RCLCPP_INFO(get_logger(), "Stopping in-progress recording...");
    finish_recording("Stopped: node deactivated", false);
  }

  RCLCPP_INFO(get_logger(), "Deactivated");
  return LifecycleNode::on_deactivate(state);
}

EpisodeRecorderNode::CallbackReturn
EpisodeRecorderNode::on_cleanup(const rclcpp_lifecycle::State &)
{
  accepting_goals_.store(false);
  cancel_pending_start("Stopped: node cleanup before recording started");
  if (is_recording_.load()) {
    finish_recording("Stopped: node cleanup", false);
  }

  // The drain thread outlives a single episode, so cleanup — not just shutdown —
  // has to join it. Otherwise it sits blocked on the condition variable with
  // writer_thread_running_ still true, and a later configure/activate cycle
  // would reuse a thread whose queue state was never reset.
  stop_writer_thread();
  {
    std::lock_guard<std::mutex> lk(queue_mutex_);
    queue_.clear();
    queue_bytes_ = 0;
    flush_requested_ = false;
    flush_done_ = false;
  }

  {
    std::lock_guard<std::mutex> lk(topics_mutex_);
    topics_.clear();
    topic_index_.clear();
  }
  action_server_.reset();
  action_cancel_client_.reset();
  start_service_.reset();
  cancel_service_.reset();
  delete_last_bag_service_.reset();
  recording_timer_.reset();
  contract_.reset();

  RCLCPP_INFO(get_logger(), "Cleaned up");
  return CallbackReturn::SUCCESS;
}

EpisodeRecorderNode::CallbackReturn
EpisodeRecorderNode::on_shutdown(const rclcpp_lifecycle::State &)
{
  accepting_goals_.store(false);
  cancel_pending_start("Stopped: node shutdown before recording started");
  if (is_recording_.load()) {
    finish_recording("Stopped: node shutdown", false);
  }
  stop_writer_thread();

  {
    std::lock_guard<std::mutex> lk(topics_mutex_);
    topics_.clear();
    topic_index_.clear();
  }
  action_server_.reset();
  recording_timer_.reset();

  RCLCPP_INFO(get_logger(), "Shutdown complete");
  return CallbackReturn::SUCCESS;
}

EpisodeRecorderNode::CallbackReturn
EpisodeRecorderNode::on_error(const rclcpp_lifecycle::State & state)
{
  RCLCPP_ERROR(get_logger(), "Error occurred in state: %s", state.label().c_str());
  accepting_goals_.store(false);
  try {
    cancel_pending_start("Stopped: node error before recording started");
    if (is_recording_.load()) {
      finish_recording("Stopped: node error", false);
    }
    close_writer();
  } catch (const std::exception & e) {
    RCLCPP_ERROR(get_logger(), "Error during cleanup: %s", e.what());
  }
  return CallbackReturn::SUCCESS;
}

// ===================== Topics =====================

void EpisodeRecorderNode::build_topic_list()
{
  std::lock_guard<std::mutex> lk(topics_mutex_);
  topics_.clear();
  topic_index_.clear();

  auto add = [this](const RecordTopic & spec) {
      // The Python node did not deduplicate, so a topic listed twice produced
      // two subscriptions and two copies of every message in the bag. Drop the
      // duplicate and say so.
      auto it = topic_index_.find(spec.topic);
      if (it != topic_index_.end()) {
        RCLCPP_WARN(
          get_logger(), "Duplicate contract topic %s ignored (already registered as %s)",
          spec.topic.c_str(), topics_[it->second]->spec.type.c_str());
        return;
      }
      auto st = std::make_unique<TopicState>();
      st->spec = spec;
      topic_index_[spec.topic] = topics_.size();
      topics_.push_back(std::move(st));
    };

  for (const auto & t : contract_->topics) {
    add(t);
  }

  // A node running on simulated time must capture /clock, or playback has
  // nothing to drive sim time with.
  if (get_parameter("use_sim_time").as_bool()) {
    RecordTopic clock;
    clock.topic = "/clock";
    clock.type = "rosgraph_msgs/msg/Clock";
    // The Python node appends /clock with a bare depth of 10, which reports
    // AUTOMATIC liveliness into the bag metadata. Match it.
    clock.qos = QosSpec{};
    clock.qos.liveliness = RMW_QOS_POLICY_LIVELINESS_AUTOMATIC;
    clock.strategy = BufferingStrategy::NoBuffer;
    add(clock);
  }
}

void EpisodeRecorderNode::create_subscription_for(size_t index)
{
  TopicState * st = topics_[index].get();
  st->sub = create_generic_subscription(
    st->spec.topic, st->spec.type, st->spec.qos.to_rclcpp(),
    [this, index](std::shared_ptr<rclcpp::SerializedMessage> msg) {
      this->on_message(index, std::move(msg));
    });
}

void EpisodeRecorderNode::discover_topics()
{
  std::vector<RecordTopic> found;
  const auto graph = get_topic_names_and_types();

  {
    std::lock_guard<std::mutex> lk(topics_mutex_);
    for (const auto & [name, types] : graph) {
      if (topic_index_.count(name)) {
        continue;
      }
      if (types.empty()) {
        continue;
      }
      if (exclude_regex_ && std::regex_search(name, *exclude_regex_)) {
        continue;
      }

      RecordTopic t;
      t.topic = name;
      t.type = types.front();
      t.strategy = BufferingStrategy::NoBuffer;

      // Match the publisher's QoS so we actually connect; fall back to the
      // rosbag2 default when nobody is publishing yet. Copy the whole profile,
      // not just the four fields a contract can express: the Python node reuses
      // the publisher's QoSProfile verbatim here, and dropping deadline /
      // lifespan / liveliness would write a profile into the bag's metadata that
      // the publisher never offered.
      const auto pubs = get_publishers_info_by_topic(name);
      if (!pubs.empty()) {
        const auto & p = pubs.front().qos_profile();
        t.qos.depth = static_cast<int>(p.depth());
        t.qos.reliable = p.reliability() == rclcpp::ReliabilityPolicy::Reliable;
        t.qos.keep_last = p.history() != rclcpp::HistoryPolicy::KeepAll;
        t.qos.transient_local = p.durability() == rclcpp::DurabilityPolicy::TransientLocal;

        t.qos.match_publisher_profile = true;
        const rmw_qos_profile_t & raw = p.get_rmw_qos_profile();
        t.qos.liveliness = static_cast<int>(raw.liveliness);
        t.qos.deadline_sec = static_cast<int64_t>(raw.deadline.sec);
        t.qos.deadline_nsec = static_cast<int64_t>(raw.deadline.nsec);
        t.qos.lifespan_sec = static_cast<int64_t>(raw.lifespan.sec);
        t.qos.lifespan_nsec = static_cast<int64_t>(raw.lifespan.nsec);
        t.qos.liveliness_lease_sec = static_cast<int64_t>(raw.liveliness_lease_duration.sec);
        t.qos.liveliness_lease_nsec = static_cast<int64_t>(raw.liveliness_lease_duration.nsec);
      }
      found.push_back(std::move(t));
    }
  }

  if (found.empty()) {
    return;
  }

  std::string names;
  {
    std::lock_guard<std::mutex> lk(topics_mutex_);
    for (const auto & t : found) {
      if (topic_index_.count(t.topic)) {
        continue;
      }
      auto st = std::make_unique<TopicState>();
      st->spec = t;
      st->discovered = true;
      const size_t idx = topics_.size();
      topic_index_[t.topic] = idx;
      topics_.push_back(std::move(st));
      create_subscription_for(idx);
      names += (names.empty() ? "" : ", ");
      names += t.topic;
    }
  }
  RCLCPP_INFO(
    get_logger(), "Auto-discovered %zu topics: [%s]", found.size(), names.c_str());
}

void EpisodeRecorderNode::destroy_discovered_subscriptions()
{
  std::lock_guard<std::mutex> lk(topics_mutex_);
  // Erase from the back so surviving indices — which live inside subscription
  // callbacks — stay valid.
  for (size_t i = topics_.size(); i-- > 0; ) {
    if (topics_[i]->discovered) {
      topic_index_.erase(topics_[i]->spec.topic);
      topics_.erase(topics_.begin() + static_cast<long>(i));
    }
  }
  // Rebuild the index: erasing shifted everything after the removed entries.
  topic_index_.clear();
  for (size_t i = 0; i < topics_.size(); ++i) {
    topic_index_[topics_[i]->spec.topic] = i;
  }
}

// ===================== Message path =====================

void EpisodeRecorderNode::on_message(
  size_t topic_index, std::shared_ptr<rclcpp::SerializedMessage> msg)
{
  const int64_t timestamp_ns = now().nanoseconds();

  // Hot path: no locks, no allocation beyond the queue node, no deserialization.
  if (!is_recording_.load(std::memory_order_acquire)) {
    std::lock_guard<std::mutex> lk(topics_mutex_);
    if (topic_index >= topics_.size()) {
      return;
    }
    TopicState * st = topics_[topic_index].get();
    // Only latched topics are worth retaining: a volatile publisher will send
    // again shortly, a latched one may never send again.
    if (!st->spec.qos.transient_local ||
      st->spec.strategy == BufferingStrategy::NoBuffer)
    {
      return;
    }
    if (msg->get_rcl_serialized_message().buffer_length > kMaxBufferBytes) {
      return;
    }
    st->buffer.push_back(BufferedMessage{std::move(msg), timestamp_ns});
    while (st->buffer.size() > static_cast<size_t>(st->spec.qos.depth)) {
      st->buffer.pop_front();
    }
    return;
  }

  const size_t nbytes = msg->get_rcl_serialized_message().buffer_length;
  {
    std::lock_guard<std::mutex> lk(queue_mutex_);
    if (queue_.size() >= kWriterQueueMaxMessages || queue_bytes_ + nbytes > kWriterQueueMaxBytes)
    {
      // Refuse to grow without bound. Dropping is bad, but an OOM kill
      // mid-episode loses the whole bag instead of one message.
      messages_dropped_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    queue_.push_back(QueuedMessage{std::move(msg), timestamp_ns, topic_index});
    queue_bytes_ += nbytes;
  }
  queue_cv_.notify_one();
}

// ===================== Writer thread =====================

void EpisodeRecorderNode::writer_thread_main()
{
  std::vector<QueuedMessage> batch;
  batch.reserve(256);

  while (true) {
    bool flushing = false;
    {
      std::unique_lock<std::mutex> lk(queue_mutex_);
      queue_cv_.wait(
        lk, [this] {
          return !queue_.empty() || flush_requested_ || !writer_thread_running_.load();
        });

      if (!writer_thread_running_.load() && queue_.empty()) {
        return;
      }

      // Drain in batches: one lock acquisition per batch instead of per message.
      const size_t take = std::min<size_t>(queue_.size(), 256);
      for (size_t i = 0; i < take; ++i) {
        queue_bytes_ -= queue_.front().data->get_rcl_serialized_message().buffer_length;
        batch.push_back(std::move(queue_.front()));
        queue_.pop_front();
      }
      flushing = flush_requested_ && queue_.empty();
    }

    if (!batch.empty()) {
      std::lock_guard<std::mutex> wl(writer_mutex_);
      if (writer_) {
        for (auto & qm : batch) {
          std::string topic_name;
          {
            std::lock_guard<std::mutex> tl(topics_mutex_);
            if (qm.topic_index >= topics_.size()) {
              continue;
            }
            topic_name = topics_[qm.topic_index]->spec.topic;
          }
          try {
            auto bag_msg = std::make_shared<rosbag2_storage::SerializedBagMessage>();
            // Alias the subscription's buffer instead of copying it; the
            // captured shared_ptr keeps it alive until rosbag2 is done.
            auto held = qm.data;
            bag_msg->serialized_data = std::shared_ptr<rcutils_uint8_array_t>(
              &held->get_rcl_serialized_message(),
              [held](rcutils_uint8_array_t *) {});
            set_bag_timestamp(*bag_msg, qm.timestamp_ns);
            bag_msg->topic_name = topic_name;
            writer_->write(bag_msg);

            messages_written_.fetch_add(1, std::memory_order_relaxed);
            std::lock_guard<std::mutex> tl(topics_mutex_);
            if (qm.topic_index < topics_.size()) {
              topics_[qm.topic_index]->written.fetch_add(1, std::memory_order_relaxed);
            }
          } catch (const std::exception & e) {
            if (!write_failed_.exchange(true)) {
              std::lock_guard<std::mutex> el(write_error_mutex_);
              write_error_ = std::string("Write failed on ") + topic_name + ": " + e.what();
              RCLCPP_ERROR(get_logger(), "%s", write_error_.c_str());
            }
            // Let the timer tick observe write_failed_ and stop the episode.
            stop_requested_.store(true);
          }
        }
      }
      batch.clear();
    }

    if (flushing) {
      std::lock_guard<std::mutex> lk(queue_mutex_);
      if (queue_.empty()) {
        flush_requested_ = false;
        flush_done_ = true;
        flush_cv_.notify_all();
      }
    }
  }
}

void EpisodeRecorderNode::stop_writer_thread()
{
  if (!writer_thread_running_.exchange(false)) {
    return;
  }
  queue_cv_.notify_all();
  if (writer_thread_.joinable()) {
    writer_thread_.join();
  }
}

// ===================== Recording =====================

fs::path EpisodeRecorderNode::create_bag_dir()
{
  // Every field comes from one reading of the clock: a separate call could land
  // in the next second and stamp a mismatched pair.
  const auto tp = std::chrono::system_clock::now();
  const auto since = tp.time_since_epoch();
  const auto secs = std::chrono::duration_cast<std::chrono::seconds>(since);
  const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(since - secs);

  std::ostringstream base;
  if (bag_name_style_ == BagNameStyle::kDateTime) {
    // YYYYMMDD-HHMMSS-mmm, so the name is final from birth and no consumer has
    // to rename it.
    const std::time_t tt = static_cast<std::time_t>(secs.count());
    std::tm tm_local{};
    localtime_r(&tt, &tm_local);

    char stamp[32];
    std::strftime(stamp, sizeof(stamp), "%Y%m%d-%H%M%S", &tm_local);

    const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(nanos);
    base << stamp << '-' << std::setfill('0') << std::setw(3) << millis.count();
  } else {
    // rosetta's original name, byte-identical to the Python node's
    // f"{sec:010d}_{nsec:09d}".
    base << std::setfill('0') << std::setw(10) << secs.count()
         << '_' << std::setw(9) << nanos.count();
  }

  fs::path dir = bag_base_ / base.str();
  for (int n = 1; fs::exists(dir); ++n) {
    dir = bag_base_ / (base.str() + "-" + std::to_string(n));
  }
  return dir;
}

size_t EpisodeRecorderNode::prepare_resubscribes()
{
  pending_resubscribed_.clear();

  // Pick up non-contract topics if asked to record everything.
  if (record_all_) {
    discover_topics();
  }

  // Recreate resubscribe_on_start subscriptions before the writer exists, so the
  // publisher's re-delivered latched sample lands in a fresh buffer and gets
  // flushed at t=0 when the bag opens.
  std::lock_guard<std::mutex> lk(topics_mutex_);
  for (size_t i = 0; i < topics_.size(); ++i) {
    if (topics_[i]->spec.strategy != BufferingStrategy::ResubscribeOnStart) {
      continue;
    }
    RCLCPP_INFO(
      get_logger(), "Re-subscribing to %s for fresh TRANSIENT_LOCAL data...",
      topics_[i]->spec.topic.c_str());
    const size_t stale = topics_[i]->buffer.size();
    topics_[i]->buffer.clear();
    if (stale) {
      RCLCPP_INFO(
        get_logger(), "Cleared %zu stale messages from %s buffer",
        stale, topics_[i]->spec.topic.c_str());
    }
    topics_[i]->sub.reset();
    create_subscription_for(i);
    pending_resubscribed_.push_back(i);
  }
  return pending_resubscribed_.size();
}

void EpisodeRecorderNode::open_writer(const fs::path & bag_dir)
{
  // Open storage and register every topic.
  rosbag2_storage::StorageOptions storage_options;
  storage_options.uri = bag_dir.string();
  storage_options.storage_id = storage_id_;

  rosbag2_cpp::ConverterOptions converter_options;
  converter_options.input_serialization_format = "cdr";
  converter_options.output_serialization_format = "cdr";

  auto writer = std::make_unique<rosbag2_cpp::Writer>();
  writer->open(storage_options, converter_options);

  {
    std::lock_guard<std::mutex> lk(topics_mutex_);
    [[maybe_unused]] uint16_t topic_id = 0;
    for (const auto & st : topics_) {
      rosbag2_storage::TopicMetadata meta;
      meta.name = st->spec.topic;
      meta.type = st->spec.type;
      meta.serialization_format = "cdr";
#ifdef ROSETTA_RECORDER_QOS_OBJECT_METADATA
      // Jazzy and newer: offered_qos_profiles is a vector<rclcpp::QoS> and the
      // metadata carries an explicit id, mirroring the _IS_JAZZY branch of the
      // Python node. Same values as the Humble YAML below.
      meta.id = topic_id;
      meta.offered_qos_profiles = {st->spec.qos.to_offered_qos()};
#else
      // Humble: a YAML string, byte-compatible with what the Python recorder has
      // written into every bag so far.
      meta.offered_qos_profiles = st->spec.qos.to_offered_qos_yaml();
#endif
      writer->create_topic(meta);
      st->written.store(0);
      ++topic_id;
    }
  }

  const int64_t bag_start_ns = now().nanoseconds();

  {
    std::lock_guard<std::mutex> wl(writer_mutex_);
    writer_ = std::move(writer);

    // Flush retained latched messages at t=0 so a player has them immediately.
    // They all share the bag-start timestamp: they are latched, so the player
    // republishes them as TRANSIENT_LOCAL regardless of their original time.
    std::lock_guard<std::mutex> tl(topics_mutex_);
    for (auto & st : topics_) {
      if (st->buffer.empty()) {
        continue;
      }
      for (auto & bm : st->buffer) {
        auto bag_msg = std::make_shared<rosbag2_storage::SerializedBagMessage>();
        auto held = bm.data;
        bag_msg->serialized_data = std::shared_ptr<rcutils_uint8_array_t>(
          &held->get_rcl_serialized_message(),
          [held](rcutils_uint8_array_t *) {});
        set_bag_timestamp(*bag_msg, bag_start_ns);
        bag_msg->topic_name = st->spec.topic;
        writer_->write(bag_msg);
        messages_written_.fetch_add(1, std::memory_order_relaxed);
        st->written.fetch_add(1, std::memory_order_relaxed);
      }
      RCLCPP_INFO(
        get_logger(), "Flushed %zu buffered messages for %s",
        st->buffer.size(), st->spec.topic.c_str());
    }
  }

  // Start the drain thread only once the writer is live.
  if (!writer_thread_running_.exchange(true)) {
    {
      std::lock_guard<std::mutex> lk(queue_mutex_);
      queue_.clear();
      queue_bytes_ = 0;
      flush_requested_ = false;
      flush_done_ = false;
    }
    writer_thread_ = std::thread(&EpisodeRecorderNode::writer_thread_main, this);
  }
}

void EpisodeRecorderNode::close_writer()
{
  std::lock_guard<std::mutex> wl(writer_mutex_);
  if (writer_) {
    // Explicit close finalizes the MCAP index; letting the destructor do it
    // hides failures.
    writer_->close();
    writer_.reset();
  }
}

void EpisodeRecorderNode::begin_episode()
{
  // Register topics and flush retained messages before live writes begin.
  open_writer(current_bag_dir_);

  recording_start_ = now();
  is_recording_.store(true, std::memory_order_release);

  const auto period = std::chrono::duration<double>(1.0 / feedback_rate_hz_);
  recording_timer_ = create_wall_timer(
    std::chrono::duration_cast<std::chrono::nanoseconds>(period),
    std::bind(&EpisodeRecorderNode::on_recording_tick, this), services_cbg_);
}

bool EpisodeRecorderNode::start_recording(const std::string & prompt, std::string & error_out)
{
  std::lock_guard<std::mutex> lk(lifecycle_mutex_);

  if (!accepting_goals_.load()) {
    error_out = "Node not active";
    return false;
  }
  if (is_recording_.load() || start_pending_.load()) {
    error_out = "Already recording";
    return false;
  }

  stop_requested_.store(false);
  write_failed_.store(false);
  {
    std::lock_guard<std::mutex> el(write_error_mutex_);
    write_error_.clear();
    stop_reason_.clear();
  }
  messages_written_.store(0);
  messages_dropped_.store(0);
  current_prompt_ = prompt;
  current_max_duration_ = default_max_duration_;
  current_bag_dir_ = create_bag_dir();

  RCLCPP_INFO(
    get_logger(), "Recording: %s, max=%.1fs",
    current_bag_dir_.c_str(), current_max_duration_);

  const size_t resubscribed = prepare_resubscribes();

  if (resubscribed == 0) {
    // Common case — nothing to wait for, so the episode goes live inline and the
    // caller learns immediately whether the bag opened.
    try {
      begin_episode();
    } catch (const std::exception & e) {
      error_out = std::string("Failed to open bag: ") + e.what();
      RCLCPP_ERROR(get_logger(), "%s", error_out.c_str());
      close_writer();
      return false;
    }
    return true;
  }

  // A latched publisher needs a moment to re-deliver to the subscriptions we
  // just recreated, and that delivery happens on the executor thread. Sleeping
  // here would block the very callbacks we are waiting for — and stall every
  // other topic for the duration — so the rest of the start runs from a one-shot
  // timer instead, leaving the executor free to spin.
  start_pending_.store(true);
  pending_start_timer_ = create_wall_timer(
    kResubscribeSettleDelay, std::bind(&EpisodeRecorderNode::complete_start, this),
    services_cbg_);
  return true;
}

void EpisodeRecorderNode::complete_start()
{
  std::lock_guard<std::mutex> lk(lifecycle_mutex_);

  if (pending_start_timer_) {
    pending_start_timer_->cancel();
    pending_start_timer_.reset();
  }
  if (!start_pending_.exchange(false)) {
    return;
  }

  {
    std::lock_guard<std::mutex> tl(topics_mutex_);
    for (size_t i : pending_resubscribed_) {
      if (i >= topics_.size()) {
        continue;
      }
      const size_t n = topics_[i]->buffer.size();
      if (n == 0) {
        RCLCPP_WARN(
          get_logger(),
          "After re-subscribe, %s buffer is EMPTY! Publisher may not use "
          "TRANSIENT_LOCAL QoS or is not running. Recording will continue "
          "without this data.", topics_[i]->spec.topic.c_str());
      } else {
        RCLCPP_INFO(
          get_logger(), "After re-subscribe, %s buffer has %zu message(s)",
          topics_[i]->spec.topic.c_str(), n);
      }
    }
  }
  pending_resubscribed_.clear();

  try {
    begin_episode();
  } catch (const std::exception & e) {
    const std::string err = std::string("Failed to open bag: ") + e.what();
    RCLCPP_ERROR(get_logger(), "%s", err.c_str());
    close_writer();
    // The goal was already accepted, so the failure has to be reported through
    // the result rather than the accept/reject response.
    if (goal_handle_) {
      auto result = std::make_shared<RecordEpisode::Result>();
      result->success = false;
      result->message = err;
      result->bag_path = "";
      result->messages_written = 0;
      goal_handle_->abort(result);
      goal_handle_.reset();
    }
  }
}

void EpisodeRecorderNode::cancel_pending_start(const std::string & reason)
{
  if (!start_pending_.exchange(false)) {
    return;
  }
  if (pending_start_timer_) {
    pending_start_timer_->cancel();
    pending_start_timer_.reset();
  }
  pending_resubscribed_.clear();
  RCLCPP_WARN(get_logger(), "%s", reason.c_str());

  if (goal_handle_) {
    auto result = std::make_shared<RecordEpisode::Result>();
    result->success = false;
    result->message = reason;
    result->bag_path = "";
    result->messages_written = 0;
    goal_handle_->abort(result);
    goal_handle_.reset();
  }
}

void EpisodeRecorderNode::on_recording_tick()
{
  if (!is_recording_.load()) {
    return;
  }

  const double elapsed = (now() - recording_start_).seconds();
  const double remaining = std::max(0.0, current_max_duration_ - elapsed);

  if (write_failed_.load()) {
    std::string err;
    {
      std::lock_guard<std::mutex> el(write_error_mutex_);
      err = write_error_;
    }
    finish_recording(err.empty() ? "Write failed" : err, false);
    return;
  }

  if (stop_requested_.load()) {
    // An action-level cancel puts the goal in CANCELING and terminates as
    // CANCELED. A stop via ~/cancel_recording cannot reach that state — only
    // the action's own cancel service can — so it terminates as SUCCEEDED with
    // the reason recorded in the message. The bag is complete either way.
    const bool canceled = goal_handle_ && goal_handle_->is_canceling();
    std::string reason;
    {
      std::lock_guard<std::mutex> el(write_error_mutex_);
      reason = stop_reason_.empty() ? "Stopped" : stop_reason_;
    }
    finish_recording(canceled ? "Cancelled" : reason, canceled);
    return;
  }

  if (goal_handle_ && goal_handle_->is_canceling()) {
    finish_recording("Cancelled", true);
    return;
  }

  if (remaining <= 0.0) {
    RCLCPP_INFO(get_logger(), "Timeout reached");
    finish_recording("Timeout reached", false);
    return;
  }

  if (goal_handle_) {
    auto fb = std::make_shared<RecordEpisode::Feedback>();
    fb->seconds_remaining = static_cast<int32_t>(remaining);
    fb->messages_written = static_cast<int32_t>(messages_written_.load());
    fb->status = "recording";
    goal_handle_->publish_feedback(fb);
  }
}

void EpisodeRecorderNode::finish_recording(const std::string & reason, bool canceled)
{
  if (!is_recording_.exchange(false)) {
    return;
  }

  if (recording_timer_) {
    recording_timer_->cancel();
    recording_timer_.reset();
  }

  const double elapsed = (now() - recording_start_).seconds();

  // Drain everything the subscriptions handed us before closing the bag,
  // otherwise the tail of the episode is silently lost.
  if (writer_thread_running_.load()) {
    std::unique_lock<std::mutex> lk(queue_mutex_);
    flush_requested_ = true;
    flush_done_ = false;
    queue_cv_.notify_all();
    if (!flush_cv_.wait_for(lk, 10s, [this] {return flush_done_ || queue_.empty();})) {
      RCLCPP_WARN(
        get_logger(), "Writer queue did not drain within 10s; %zu messages abandoned",
        queue_.size());
      // Drop what is left rather than carrying it into the next episode's bag —
      // the writer thread outlives a single recording.
      queue_.clear();
      queue_bytes_ = 0;
    }
    flush_requested_ = false;
  }

  close_writer();
  destroy_discovered_subscriptions();

  const uint64_t written = messages_written_.load();
  const uint64_t dropped = messages_dropped_.load();

  std::string metadata_error;
  try {
    write_metadata(current_bag_dir_, current_prompt_);
  } catch (const std::exception & e) {
    metadata_error = e.what();
    RCLCPP_ERROR(get_logger(), "Metadata error: %s", metadata_error.c_str());
  }

  last_bag_dir_ = current_bag_dir_;

  // Per-topic summary, so a silent topic is visible without `ros2 bag info`.
  std::string lines;
  {
    std::lock_guard<std::mutex> lk(topics_mutex_);
    for (const auto & st : topics_) {
      const uint64_t c = st->written.load();
      if (st->discovered && c == 0) {
        continue;
      }
      lines += "  " + st->spec.topic + ": " + std::to_string(c) + (c == 0 ? " (!)" : "") + "\n";
    }
  }
  RCLCPP_INFO(
    get_logger(), "Recorded %lu messages to %s (%.1fs)\n%s",
    static_cast<unsigned long>(written), current_bag_dir_.c_str(), elapsed, lines.c_str());
  if (dropped) {
    RCLCPP_ERROR(
      get_logger(), "Dropped %lu messages: writer queue was full",
      static_cast<unsigned long>(dropped));
  }

  // Settle the action goal, if this episode came from one.
  if (goal_handle_) {
    auto result = std::make_shared<RecordEpisode::Result>();
    result->bag_path = current_bag_dir_.string();
    result->messages_written = static_cast<int32_t>(written);

    if (!metadata_error.empty()) {
      result->success = false;
      result->message = "Recording completed but metadata failed: " + metadata_error;
      goal_handle_->abort(result);
    } else if (canceled) {
      result->success = false;
      result->message = "Cancelled";
      goal_handle_->canceled(result);
    } else if (write_failed_.load()) {
      result->success = false;
      result->message = reason;
      goal_handle_->abort(result);
    } else {
      result->success = true;
      result->message = "Recorded " + std::to_string(written) + " messages (" + reason + ")";
      if (dropped) {
        result->message += " (" + std::to_string(dropped) + " dropped: writer queue full)";
      }
      goal_handle_->succeed(result);
    }
    goal_handle_.reset();
  }
}

void EpisodeRecorderNode::write_metadata(const fs::path & bag_dir, const std::string & prompt)
{
  if (prompt.empty()) {
    return;
  }

  const fs::path meta_path = bag_dir / "metadata.yaml";
  std::string last_error = "metadata.yaml never appeared";

  for (int attempt = 0; attempt < kMetadataRetryCount; ++attempt) {
    try {
      if (!fs::exists(meta_path)) {
        std::this_thread::sleep_for(kMetadataRetryDelay);
        continue;
      }

      YAML::Node meta = YAML::LoadFile(meta_path.string());
      if (!meta.IsMap()) {
        meta = YAML::Node(YAML::NodeType::Map);
      }
      if (!meta[kBagMetadataKey] || !meta[kBagMetadataKey].IsMap()) {
        meta[kBagMetadataKey] = YAML::Node(YAML::NodeType::Map);
      }
      YAML::Node info = meta[kBagMetadataKey];
      if (!info[kBagCustomDataKey] || !info[kBagCustomDataKey].IsMap()) {
        info[kBagCustomDataKey] = YAML::Node(YAML::NodeType::Map);
      }
      info[kBagCustomDataKey][kBagPromptKey] = prompt;

      std::ofstream out(meta_path);
      if (!out) {
        throw std::runtime_error("cannot open metadata.yaml for writing");
      }
      out << meta;
      out << "\n";
      out.close();
      if (!out) {
        throw std::runtime_error("failed to flush metadata.yaml");
      }

      RCLCPP_DEBUG(get_logger(), "Wrote prompt to metadata on attempt %d", attempt + 1);
      return;
    } catch (const std::exception & e) {
      last_error = e.what();
      RCLCPP_DEBUG(
        get_logger(), "Metadata write attempt %d failed: %s", attempt + 1, last_error.c_str());
      std::this_thread::sleep_for(kMetadataRetryDelay);
    }
  }

  // Fail loudly rather than silently losing the operator's prompt.
  throw std::runtime_error(
    "Failed to write prompt to " + meta_path.string() + " after " +
    std::to_string(kMetadataRetryCount) + " attempts. Last error: " + last_error);
}

// ===================== Action callbacks =====================

rclcpp_action::GoalResponse EpisodeRecorderNode::handle_goal(
  const rclcpp_action::GoalUUID &, std::shared_ptr<const RecordEpisode::Goal>)
{
  RCLCPP_INFO(get_logger(), "Received goal request");
  if (!accepting_goals_.load()) {
    RCLCPP_WARN(get_logger(), "Rejected: node not active");
    return rclcpp_action::GoalResponse::REJECT;
  }
  if (is_recording_.load() || start_pending_.load()) {
    RCLCPP_WARN(get_logger(), "Rejected: already recording");
    return rclcpp_action::GoalResponse::REJECT;
  }
  RCLCPP_INFO(get_logger(), "Goal accepted");
  return rclcpp_action::GoalResponse::ACCEPT_AND_EXECUTE;
}

rclcpp_action::CancelResponse EpisodeRecorderNode::handle_cancel(std::shared_ptr<GoalHandle>)
{
  RCLCPP_INFO(get_logger(), "Received cancel request");
  stop_requested_.store(true);
  return rclcpp_action::CancelResponse::ACCEPT;
}

void EpisodeRecorderNode::handle_accepted(std::shared_ptr<GoalHandle> handle)
{
  // Returns immediately. Unlike the Python node there is no worker thread
  // sleeping in a loop here: the timer created by start_recording() drives
  // feedback, timeout and completion.
  goal_handle_ = handle;

  std::string error;
  if (!start_recording(handle->get_goal()->prompt, error)) {
    auto result = std::make_shared<RecordEpisode::Result>();
    result->success = false;
    result->message = error;
    result->bag_path = "";
    result->messages_written = 0;
    handle->abort(result);
    goal_handle_.reset();
  }
}

// ===================== Service callbacks =====================

void EpisodeRecorderNode::on_start_service(
  const std::shared_ptr<StartRecording::Request> req,
  std::shared_ptr<StartRecording::Response> res)
{
  RCLCPP_INFO(get_logger(), "start_recording service called: prompt='%s'", req->prompt.c_str());

  std::string error;
  // No action goal for service-based recording; goal_handle_ stays null and the
  // timer simply skips feedback.
  goal_handle_.reset();
  if (start_recording(req->prompt, error)) {
    res->accepted = true;
    res->message = "Recording started";
  } else {
    res->accepted = false;
    res->message = error;
  }
}

bool EpisodeRecorderNode::request_action_cancel()
{
  if (!goal_handle_ || !action_cancel_client_) {
    return false;
  }
  if (goal_handle_->is_canceling()) {
    // Already on its way to CANCELED; nothing more to ask for.
    return true;
  }
  if (!action_cancel_client_->service_is_ready()) {
    RCLCPP_WARN(
      get_logger(),
      "Action cancel service not ready; stopping the episode directly, which "
      "terminates the goal as SUCCEEDED rather than CANCELED");
    return false;
  }

  auto req = std::make_shared<action_msgs::srv::CancelGoal::Request>();
  const rclcpp_action::GoalUUID & uuid = goal_handle_->get_goal_id();
  std::copy(uuid.begin(), uuid.end(), req->goal_info.goal_id.uuid.begin());

  action_cancel_client_->async_send_request(
    req,
    [this](rclcpp::Client<action_msgs::srv::CancelGoal>::SharedFuture future) {
      const auto response = future.get();
      if (response->return_code == action_msgs::srv::CancelGoal::Response::ERROR_NONE &&
        !response->goals_canceling.empty())
      {
        return;
      }
      // The action server refused the cancel (goal already terminal, or gone).
      // Fall back so a stop request is never silently dropped.
      RCLCPP_WARN(
        get_logger(), "Action cancel was not accepted (code %d); stopping directly",
        static_cast<int>(response->return_code));
      std::lock_guard<std::mutex> el(write_error_mutex_);
      stop_reason_ = "Stopped by cancel_recording service";
      stop_requested_.store(true);
    });
  return true;
}

void EpisodeRecorderNode::on_cancel_service(
  const std::shared_ptr<Trigger::Request>, std::shared_ptr<Trigger::Response> res)
{
  if (!is_recording_.load() && !start_pending_.load()) {
    res->success = false;
    res->message = "No active recording";
    return;
  }

  RCLCPP_INFO(get_logger(), "cancel_recording service called: stopping recording");
  {
    std::lock_guard<std::mutex> el(write_error_mutex_);
    stop_reason_ = "Stopped by cancel_recording service";
  }

  // Prefer the action's own cancel path: it is the only route to the CANCELING
  // state, and therefore the only way this episode terminates as CANCELED like
  // the Python node's did.
  // Without a goal (service-started episode) there is nothing to cancel, so stop
  // directly.
  if (!request_action_cancel()) {
    stop_requested_.store(true);
  }

  res->success = true;
  res->message = "Cancel requested";
}

void EpisodeRecorderNode::on_delete_last_bag_service(
  const std::shared_ptr<Trigger::Request>, std::shared_ptr<Trigger::Response> res)
{
  if (is_recording_.load()) {
    res->success = false;
    res->message = "Cannot delete: recording in progress";
    return;
  }
  if (last_bag_dir_.empty()) {
    res->success = false;
    res->message = "No bag to delete";
    return;
  }

  std::error_code ec;
  if (!fs::exists(last_bag_dir_)) {
    res->success = false;
    res->message = "Bag path not found: " + last_bag_dir_.string();
    return;
  }

  fs::remove_all(last_bag_dir_, ec);
  if (ec) {
    RCLCPP_ERROR(
      get_logger(), "Failed to delete bag %s: %s",
      last_bag_dir_.c_str(), ec.message().c_str());
    res->success = false;
    res->message = "Delete failed: " + ec.message();
    return;
  }

  RCLCPP_INFO(get_logger(), "Deleted bag: %s", last_bag_dir_.c_str());
  res->success = true;
  res->message = "Deleted: " + last_bag_dir_.filename().string();
  last_bag_dir_.clear();
}

}  // namespace rosetta_recorder_cpp
