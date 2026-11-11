cmake_minimum_required(VERSION 3.16)
project(raftkv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()


if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-UNDEBUG)
endif()

find_package(Threads REQUIRED)

option(RAFTKV_SANITIZE "Build with ASan/UBSan" OFF)
if(RAFTKV_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

option(RAFTKV_TSAN "Build with ThreadSanitizer" OFF)
if(RAFTKV_TSAN)
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
endif()


set(RAFTKV_CORE_SOURCES
    src/common/log.cc
    src/common/thread_pool.cc
)

add_library(raftkv_core STATIC ${RAFTKV_CORE_SOURCES})
target_include_directories(raftkv_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(raftkv_core PUBLIC Threads::Threads)
target_compile_options(raftkv_core PRIVATE -Wall -Wextra -Wno-unused-parameter)

add_executable(raftkv-server src/server/main.cc)
target_link_libraries(raftkv-server PRIVATE raftkv_core)
target_compile_options(raftkv-server PRIVATE -Wall -Wextra -Wno-unused-parameter)

add_executable(raftkv-cli src/client/main.cc)
target_link_libraries(raftkv-cli PRIVATE raftkv_core)
target_compile_options(raftkv-cli PRIVATE -Wall -Wextra -Wno-unused-parameter)

add_executable(raftkv-tests
    tests/test_main.cc
    tests/test_codec.cc
)
target_include_directories(raftkv-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
target_link_libraries(raftkv-tests PRIVATE raftkv_core)
target_compile_options(raftkv-tests PRIVATE -Wall -Wextra -Wno-unused-parameter)

enable_testing()
add_test(NAME raftkv_unit COMMAND raftkv-tests)
set_tests_properties(raftkv_unit PROPERTIES TIMEOUT 120)
