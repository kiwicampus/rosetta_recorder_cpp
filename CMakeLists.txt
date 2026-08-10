cmake_minimum_required(VERSION 3.8)
project(rosetta_recorder_cpp)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(action_msgs REQUIRED)
find_package(rclcpp REQUIRED)
find_package(rclcpp_action REQUIRED)
find_package(rclcpp_lifecycle REQUIRED)
find_package(lifecycle_msgs REQUIRED)
find_package(rosbag2_cpp REQUIRED)
find_package(rosbag2_storage REQUIRED)
find_package(std_msgs REQUIRED)
find_package(std_srvs REQUIRED)
find_package(rosetta_interfaces REQUIRED)
find_package(yaml-cpp REQUIRED)

# ---------------------------------------------------------------------------
# The recorder
#
# The node lives in a library so the tests can link it directly; the executable
# is just main().
# ---------------------------------------------------------------------------
set(ROSETTA_RECORDER_DEPS
  action_msgs
  rclcpp
  rclcpp_action
  rclcpp_lifecycle
  lifecycle_msgs
  rosbag2_cpp
  rosbag2_storage
  std_msgs
  std_srvs
  rosetta_interfaces
)

add_library(rosetta_recorder_core
  src/episode_recorder_node.cpp
  src/contract.cpp
)
target_include_directories(rosetta_recorder_core PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
target_compile_features(rosetta_recorder_core PUBLIC cxx_std_17)
ament_target_dependencies(rosetta_recorder_core ${ROSETTA_RECORDER_DEPS})
target_link_libraries(rosetta_recorder_core yaml-cpp)

# rosbag2 made two breaking struct changes in Jazzy, both on the same distro
# boundary. TopicMetadata: offered_qos_profiles went from a YAML string to a
# std::vector<rclcpp::QoS>, and the struct gained an id. SerializedBagMessage:
# time_stamp was split into recv_timestamp and send_timestamp. This is the C++
# equivalent of the Python node's is_jazzy_or_newer() branch, and it is resolved
# the same way — from ROS_DISTRO.
if(DEFINED ENV{ROS_DISTRO})
  set(_erc_ros_distro "$ENV{ROS_DISTRO}")
else()
  set(_erc_ros_distro "humble")
endif()
if(NOT _erc_ros_distro MATCHES "^(foxy|galactic|humble|iron)$")
  message(
    STATUS
    "rosetta_recorder_cpp: ROS_DISTRO=${_erc_ros_distro} -> QoS object topic metadata, split bag timestamps")
  target_compile_definitions(rosetta_recorder_core PUBLIC
    ROSETTA_RECORDER_QOS_OBJECT_METADATA=1
    ROSETTA_RECORDER_TIMESTAMP_SPLIT=1
  )
else()
  message(
    STATUS
    "rosetta_recorder_cpp: ROS_DISTRO=${_erc_ros_distro} -> YAML string topic metadata, single bag timestamp")
endif()

add_executable(episode_recorder_node src/main.cpp)
target_compile_features(episode_recorder_node PUBLIC cxx_std_17)
target_link_libraries(episode_recorder_node rosetta_recorder_core)
ament_target_dependencies(episode_recorder_node rclcpp)

install(TARGETS rosetta_recorder_core
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)
install(TARGETS episode_recorder_node
  DESTINATION lib/${PROJECT_NAME}
)
install(DIRECTORY include/
  DESTINATION include
)
install(DIRECTORY launch/
  DESTINATION share/${PROJECT_NAME}/launch
)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
if(BUILD_TESTING)
  find_package(ament_cmake_gtest REQUIRED)

  # Contract loading + QoS serialization. No ROS graph involved.
  ament_add_gtest(test_contract test/test_contract.cpp)
  target_link_libraries(test_contract rosetta_recorder_core)

  # Lifecycle, services and a real end-to-end recording. Runs on its own
  # ROS_DOMAIN_ID so it can never see, or be seen by, a robot on the default one.
  ament_add_gtest(test_recorder_node test/test_recorder_node.cpp
    ENV ROS_DOMAIN_ID=93
    TIMEOUT 180
  )
  target_link_libraries(test_recorder_node rosetta_recorder_core)
endif()

ament_package()
