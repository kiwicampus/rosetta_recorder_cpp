# Copyright 2025 Isaac Blankenau
# Copyright 2026 Robot.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Derived from rosetta's episode_recorder_node.py (Apache-2.0,
# Copyright 2025 Isaac Blankenau). See NOTICE for what changed.

"""
Standalone launch for the C++ episode recorder.

Brings the node up and drives it configure -> activate, the way a lifecycle
manager normally would. Use this to run the recorder on its own.

To swap it in for rosetta's Python recorder in an existing launch file, change
`package="rosetta"` to `package="rosetta_recorder_cpp"` on the episode_recorder
LifecycleNode — node name, parameters, action and service names are unchanged.
"""

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, EmitEvent, RegisterEventHandler
from launch.event_handlers import OnProcessStart
from launch.events import matches_action
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import LifecycleNode
from launch_ros.event_handlers import OnStateTransition
from launch_ros.events.lifecycle import ChangeState
from lifecycle_msgs.msg import Transition


def generate_launch_description():
    """Build the launch description for the C++ episode recorder."""
    recorder = LifecycleNode(
        package="rosetta_recorder_cpp",
        executable="episode_recorder_node",
        name="episode_recorder",
        namespace="",
        output="screen",
        parameters=[
            {
                "contract_path": LaunchConfiguration("contract_path"),
                "bag_base_dir": LaunchConfiguration("bag_base_dir"),
                "storage_id": LaunchConfiguration("storage_id"),
                "default_max_duration": LaunchConfiguration("default_max_duration"),
                "feedback_rate_hz": LaunchConfiguration("feedback_rate_hz"),
                "record_all": LaunchConfiguration("record_all"),
                "bag_name_style": LaunchConfiguration("bag_name_style"),
            }
        ],
    )

    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "contract_path",
                description="Path to the contract YAML describing what to record",
            ),
            DeclareLaunchArgument(
                "bag_base_dir",
                default_value="/data/rosbags/recording",
                description="Directory new bags are created under",
            ),
            DeclareLaunchArgument(
                "storage_id",
                default_value="mcap",
                description="rosbag2 storage plugin (mcap, sqlite3)",
            ),
            DeclareLaunchArgument(
                "default_max_duration",
                default_value="600.0",
                description="Auto-stop an episode after this many seconds",
            ),
            DeclareLaunchArgument(
                "feedback_rate_hz",
                default_value="2.0",
                description="RecordEpisode action feedback rate in Hz",
            ),
            DeclareLaunchArgument(
                "record_all",
                default_value="false",
                description="Also record non-contract topics found on the graph",
            ),
            DeclareLaunchArgument(
                "bag_name_style",
                default_value="epoch",
                description=(
                    "Bag directory naming: 'epoch' for rosetta's "
                    "<epoch_sec>_<nsec>, or 'datetime' for "
                    "YYYYMMDD-HHMMSS-mmm"
                ),
            ),
            recorder,
            # configure once the process is up, then activate once it reports inactive
            RegisterEventHandler(
                OnProcessStart(
                    target_action=recorder,
                    on_start=[
                        EmitEvent(
                            event=ChangeState(
                                lifecycle_node_matcher=matches_action(recorder),
                                transition_id=Transition.TRANSITION_CONFIGURE,
                            ),
                        ),
                    ],
                ),
            ),
            RegisterEventHandler(
                OnStateTransition(
                    target_lifecycle_node=recorder,
                    goal_state="inactive",
                    entities=[
                        EmitEvent(
                            event=ChangeState(
                                lifecycle_node_matcher=matches_action(recorder),
                                transition_id=Transition.TRANSITION_ACTIVATE,
                            ),
                        ),
                    ],
                ),
            ),
        ]
    )
