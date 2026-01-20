cmake_minimum_required(VERSION 3.10)
project(tpch_query5 LANGUAGES CXX)

# Use modern standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE OFF)

# Default to Release when no configuration is given (single-config generators)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

# Sources: adjust paths if your files are not under src/
set(SOURCES
    src/main.cpp
    src/query5.cpp
)

add_executable(tpch_query5 ${SOURCES})

# Include headers directory (if you keep query5.hpp under include/)
target_include_directories(tpch_query5 PRIVATE include)

# Find and link threads
find_package(Threads REQUIRED)
target_link_libraries(tpch_query5 PRIVATE Threads::Threads)

# Compiler options
if(MSVC)
  # Use generator expressions so options are applied per-configuration and don't conflict.
  target_compile_options(tpch_query5 PRIVATE
    # Release: enable optimizations
    $<$<CONFIG:Release>:/O2 /Gy /GL>
    # Debug: keep runtime checks and debug info
    $<$<CONFIG:Debug>:/Od /RTC1 /Zi>
    # Warnings for all configs
    /W4
  )
  # Link-time code generation for Release only
  target_link_options(tpch_query5 PRIVATE $<$<CONFIG:Release>:/LTCG>)
else()
  # GCC/Clang: enable high optimization and link-time optimization only for Release
  target_compile_options(tpch_query5 PRIVATE
    -Wall -Wextra -Wpedantic
    $<$<CONFIG:Release>:-O3 -march=native -flto -funroll-loops -fomit-frame-pointer>
  )
  target_link_options(tpch_query5 PRIVATE $<$<CONFIG:Release>:-flto>)
endif()

# Install target (optional)
# install(TARGETS tpch_query5 RUNTIME DESTINATION bin)