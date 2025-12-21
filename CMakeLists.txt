cmake_minimum_required(VERSION 3.16)

project(element_redis LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

# Default build type if not specified
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# ---------- Dependencies ----------
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

# ---------- Include paths ----------
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${HIREDIS_INCLUDE_DIRS})

# ---------- Core library ----------
# Grab all .cpp from src/ and exclude er_abi.cpp (ABI wrapper compiled separately)
file(GLOB CORE_SOURCES CONFIGURE_DEPENDS
  ${CMAKE_SOURCE_DIR}/src/*.cpp
)

# Remove ABI wrapper if it lives in src/
list(FILTER CORE_SOURCES EXCLUDE REGEX ".*/er_abi\\.cpp$")

add_library(er_core STATIC ${CORE_SOURCES})

set_target_properties(er_core PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(er_core PUBLIC
  ${CMAKE_SOURCE_DIR}/include
  ${HIREDIS_INCLUDE_DIRS}
)

target_compile_options(er_core PRIVATE ${HIREDIS_CFLAGS_OTHER})
target_link_libraries(er_core PUBLIC ${HIREDIS_LIBRARIES})

# ---------- CLI ----------
# Adjust if you have more CLI files; this assumes cli/er_cli.cpp is the entry.
add_executable(er_cli
  cli/er_cli.cpp
)

target_include_directories(er_cli PRIVATE
  ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(er_cli PRIVATE er_core)

# Put binary in build/cli/ like you already have
set_target_properties(er_cli PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/cli
)

# ---------- ABI shared library (.so) ----------
add_library(er_abi SHARED
  src/er_abi.cpp
)

target_include_directories(er_abi PUBLIC
  ${CMAKE_SOURCE_DIR}/include
)

# For Windows later; harmless on Linux
target_compile_definitions(er_abi PRIVATE ER_ABI_EXPORTS)

target_link_libraries(er_abi PRIVATE er_core)

# Optional: make sure the .so ends up in build/ (default) or a known folder
# set_target_properties(er_abi PROPERTIES
#   LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
# )

