cmake_minimum_required(VERSION 3.10)
project(inverted_index)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Manually specify LibArchive paths using the correct prefix
set(LibArchive_INCLUDE_DIR "/opt/homebrew/opt/libarchive/include")
set(LibArchive_LIBRARY "/opt/homebrew/opt/libarchive/lib/libarchive.dylib")

# Check ZLIB package
find_package(ZLIB REQUIRED)

# Set source directory
set(SOURCE_DIR src)

# Add executable
add_executable(build_index ${SOURCE_DIR}/build_index.cpp)
add_executable(varbyte_encode_test ${SOURCE_DIR}/varbyte_encode_test.cpp)
add_executable(search ${SOURCE_DIR}/search_engine.cpp)

# Link the LibArchive library
target_include_directories(build_index PRIVATE ${LibArchive_INCLUDE_DIR})
target_link_libraries(build_index PRIVATE ${LibArchive_LIBRARY})

# Link the Zlib library
target_link_libraries(build_index PRIVATE ${ZLIB_LIBRARIES})
target_link_libraries(search PRIVATE ${ZLIB_LIBRARIES})

