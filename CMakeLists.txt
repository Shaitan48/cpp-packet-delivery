cmake_minimum_required(VERSION 3.16)
project(pdq LANGUAGES CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

if(MSVC)
    add_compile_options(/W4 /permissive-)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(Threads REQUIRED)

add_library(pdq
    src/packet.cpp
    src/delivery_manager.cpp)
target_include_directories(pdq PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(pdq PUBLIC Threads::Threads)

add_executable(pdq_demo examples/demo.cpp)
target_link_libraries(pdq_demo PRIVATE pdq)

enable_testing()
add_executable(pdq_tests tests/test_delivery.cpp)
target_include_directories(pdq_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
target_link_libraries(pdq_tests PRIVATE pdq)
add_test(NAME pdq_tests COMMAND pdq_tests)
