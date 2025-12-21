cmake_minimum_required(VERSION 3.20)
project(element_redis LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

add_library(er_core
    src/Element.cpp
    src/Flags4096.cpp
    src/RedisClient.cpp
)

target_include_directories(er_core
    PUBLIC include
    PRIVATE ${HIREDIS_INCLUDE_DIRS}
)

target_link_libraries(er_core
    PRIVATE ${HIREDIS_LIBRARIES}
)

add_subdirectory(cli)

