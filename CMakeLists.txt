cmake_minimum_required(VERSION 3.9)
project(cornerstone VERSION 0.0.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CheckIPOSupported)

option(BUILD_TESTS "Build unit testr" ON)

add_definitions(-DASIO_STANDALONE)
add_definitions(-DASIO_HAS_STD_CHRONO)

# ASIO requires threads
find_package(Threads REQUIRED)

# Builds default to RELEASE
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE RELEASE CACHE STRING
            "Choose the type of build, options are: None Debug Release."
            FORCE)
endif(NOT CMAKE_BUILD_TYPE)

# Set default optimizations
if(CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "-Wall -Wextra")
    set(CMAKE_CXX_FLAGS_DEBUG "-g")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3")
endif()

# If no installation prefix is given manually, install locally.
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/install" CACHE STRING
            "The install location"
            FORCE)
endif (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

set(SOURCE_FILES
        src/asio_service.cxx
        src/buffer.cxx
        src/cluster_config.cxx
        src/fs_log_store.cxx
        src/peer.cxx
        src/raft_server.cxx
        src/snapshot.cxx
        src/snapshot_sync_req.cxx
        src/srv_config.cxx)

set(TEST_SOURCE_FILES
        tests/src/test_async_result.cxx
        tests/src/test_buffer.cxx
        tests/src/test_everything_together.cxx
        tests/src/test_impls.cxx
        tests/src/test_log_store.cxx
        tests/src/test_logger.cxx
        tests/src/test_ptr.cxx
        tests/src/test_runner.cxx
        tests/src/test_scheduler.cxx
        tests/src/test_serialization.cxx
        tests/src/test_strfmt.cxx)

set(HEADER_FILES
        include/asio_service.hxx
        include/async.hxx
        include/basic_types.hxx
        include/buffer.hxx
        include/cluster_config.hxx
        include/context.hxx
        include/cornerstone.hxx
        include/delayed_task.hxx
        include/delayed_task_scheduler.hxx
        include/fs_log_store.hxx
        include/log_entry.hxx
        include/log_store.hxx
        include/log_val_type.hxx
        include/logger.hxx
        include/msg_base.hxx
        include/msg_type.hxx
        include/peer.hxx
        include/pp_util.hxx
        include/ptr.hxx
        include/raft_params.hxx
        include/raft_server.hxx
        include/req_msg.hxx
        include/resp_msg.hxx
        include/rpc_cli.hxx
        include/rpc_cli_factory.hxx
        include/rpc_exception.hxx
        include/rpc_listener.hxx
        include/snapshot.hxx
        include/snapshot_sync_ctx.hxx
        include/snapshot_sync_req.hxx
        include/srv_config.hxx
        include/srv_role.hxx
        include/srv_state.hxx
        include/state_machine.hxx
        include/state_mgr.hxx
        include/strfmt.hxx
        include/timer_task.hxx)

add_library(cornerstone
        ${SOURCE_FILES}
        ${HEADER_FILES})

add_library(cornerstone::cornerstone ALIAS cornerstone)

if (BUILD_TESTS)
    enable_testing()
    add_executable(testr
            ${TEST_SOURCE_FILES}
            ${HEADER_FILES})

    add_test(NAME testr COMMAND $<TARGET_FILE:testr>)
endif (BUILD_TESTS)

target_link_libraries(cornerstone
        # asio
        Threads::Threads
        )

if (BUILD_TESTS)
    target_link_libraries(testr
            cornerstone)
endif (BUILD_TESTS)

set(PROJECT_PREFIX cornerstone-${cornerstone_VERSION})

target_include_directories(cornerstone PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/${PROJECT_PREFIX}>
        PRIVATE src ${CMAKE_CURRENT_SOURCE_DIR}/asio/asio/include)

if (BUILD_TESTS)
    target_include_directories(testr PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include
            PRIVATE src ${CMAKE_CURRENT_SOURCE_DIR}/asio/asio/include)
endif (BUILD_TESTS)

set(PUBLIC_COMPILE_FEATURES
        cxx_std_17
        cxx_override
        cxx_nullptr)

target_compile_features(cornerstone
        PUBLIC ${PUBLIC_COMPILE_FEATURES})

if (BUILD_TESTS)
    target_compile_features(testr
            PUBLIC ${PUBLIC_COMPILE_FEATURES})
endif (BUILD_TESTS)

set(INSTALL_RUNTIME_DIR ${CMAKE_INSTALL_BINDIR})
set(INSTALL_CONFIG_DIR  ${CMAKE_INSTALL_LIBDIR}/${PROJECT_PREFIX}/cmake)
set(INSTALL_LIBRARY_DIR ${CMAKE_INSTALL_LIBDIR}/${PROJECT_PREFIX}/${CMAKE_BUILD_TYPE})
set(INSTALL_ARCHIVE_DIR ${CMAKE_INSTALL_LIBDIR}/${PROJECT_PREFIX}/${CMAKE_BUILD_TYPE}/static)
set(INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_PREFIX}/cornerstone)

set(PROJECT_CONFIG_VERSION_FILE "${PROJECT_BINARY_DIR}/cornerstone-config-version.cmake")
set(PROJECT_CONFIG_FILE         "${PROJECT_BINARY_DIR}/cornerstone-config.cmake")

configure_package_config_file(cmake/cornerstone-config.cmake.in
        ${PROJECT_CONFIG_FILE}
        INSTALL_DESTINATION ${INSTALL_CONFIG_DIR})

write_basic_package_version_file(
        ${PROJECT_CONFIG_VERSION_FILE}
        COMPATIBILITY SameMajorVersion)

install(TARGETS cornerstone
        EXPORT cornerstone-targets
        RUNTIME DESTINATION ${INSTALL_RUNTIME_DIR}
        LIBRARY DESTINATION ${INSTALL_LIBRARY_DIR}
        ARCHIVE DESTINATION ${INSTALL_ARCHIVE_DIR})

install(FILES ${HEADER_FILES}
        DESTINATION ${INSTALL_INCLUDE_DIR})

install(FILES
        ${PROJECT_CONFIG_VERSION_FILE}
        ${PROJECT_CONFIG_FILE}
        DESTINATION ${INSTALL_CONFIG_DIR})

install(EXPORT cornerstone-targets
        FILE cornerstone-targets.cmake
        NAMESPACE cornerstone::
        DESTINATION ${INSTALL_CONFIG_DIR})

export(EXPORT cornerstone-targets
        FILE ${CMAKE_CURRENT_BINARY_DIR}/cornerstone-targets.cmake
        NAMESPACE foo::)

export(PACKAGE cornerstone)

check_ipo_supported(RESULT ipo_supported)
if(ipo_supported)
    set_property(TARGET cornerstone PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

find_program(CCACHE_FOUND "ccache")
if(CCACHE_FOUND)
    set_property(TARGET cornerstone PROPERTY RULE_LAUNCH_COMPILE ccache)
    if(BUILD_TESTS)
        set_property(TARGET testr PROPERTY RULE_LAUNCH_LINK ccache)
    endif(BUILD_TESTS)
endif(CCACHE_FOUND)