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

#include <cstdio>
#include <memory>

#include "rclcpp/rclcpp.hpp"

#include "rosetta_recorder_cpp/episode_recorder_node.hpp"

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);

  auto node = std::make_shared<rosetta_recorder_cpp::EpisodeRecorderNode>();

  // A single executor thread is both cheaper and sufficient: nothing in this
  // node blocks it. Disk writes live on the writer thread, and the action goal
  // is driven by a timer rather than a sleeping loop.
  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(node->get_node_base_interface());

  try {
    executor.spin();
  } catch (const std::exception & e) {
    std::fprintf(stderr, "episode_recorder_node: %s\n", e.what());
  }

  executor.remove_node(node->get_node_base_interface());
  node.reset();
  rclcpp::shutdown();
  return 0;
}
