cmake_minimum_required(VERSION 3.21.3)

if(POLICY CMP0177)
    cmake_policy(SET CMP0177 NEW)
endif()

# Set extension name here
set(TARGET_NAME bigquery)

# Compile and install google-cloud-cpp depedencies
# message("Installing google-cloud-cpp dependencies")
# execute_process(
#     COMMAND
#         ${CMAKE_COMMAND}
#         -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
#         -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
#         # osx options
#         -DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
#         -DCMAKE_OSX_ARCHITECTURES=${DCMAKE_OSX_ARCHITECTURES}
#         -DCMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}
#         # vcpkg options
#         -DVCPKG_MANIFEST_DIR=${VCPKG_MANIFEST_DIR}
#         -DVCPKG_INSTALLED_DIR=${CMAKE_BINARY_DIR}/vcpkg_installed
#         -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}
#         -DVCPKG_TARGET_ARCHITECTURE=${VCPKG_TARGET_ARCHITECTURE}
#         -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
#         -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
#         -G ${CMAKE_GENERATOR}
#         -S ${CMAKE_CURRENT_SOURCE_DIR}/external
#         -B ${CMAKE_BINARY_DIR}/external
#     RESULT_VARIABLE DEPENDENCIES_GENERATE_RESULT
# )

# if(NOT DEPENDENCIES_GENERATE_RESULT EQUAL 0)
#     message(FATAL_ERROR "Could not generate dependencies project")
# endif()

# execute_process(
#     COMMAND
#         ${CMAKE_COMMAND}
#         --build ${CMAKE_BINARY_DIR}/external
#         --config ${CMAKE_BUILD_TYPE}
#     RESULT_VARIABLE DEPENDENCIES_BUILD_RESULT
# )

# if(NOT DEPENDENCIES_BUILD_RESULT EQUAL 0)
#     message(FATAL_ERROR "Could not build dependencies project")
# endif()

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${CMAKE_BINARY_DIR}/external/local)

# Find required packages
find_package(absl CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(Arrow REQUIRED)
find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)

find_package(google_cloud_cpp_common REQUIRED)
find_package(google_cloud_cpp_rest_internal REQUIRED)
find_package(google_cloud_cpp_grpc_utils REQUIRED)
find_package(google_cloud_cpp_bigquery REQUIRED)
find_package(google_cloud_cpp_bigquerycontrol REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(INCLUDE_VC_DIR ${CMAKE_BINARY_DIR}/vcpkg_installed/${VCPKG_TARGET_TRIPLET}/include/)

project(${TARGET_NAME})
include_directories(src/include)
include_directories(${INCLUDE_VC_DIR})
add_subdirectory(src)

# Configure CMake to find dependencies
include(${CMAKE_TOOLCHAIN_FILE})

# Define static and loadable extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link libraries to the extensions
function(link_libraries_to_extension target_name)
    target_link_libraries(
        ${target_name}
        OpenSSL::SSL
        OpenSSL::Crypto
        -lpthread
        -ldl
        CURL::libcurl
        google-cloud-cpp::common
        google-cloud-cpp::rest_internal
        google-cloud-cpp::bigquery
        google-cloud-cpp::bigquerycontrol
        google-cloud-cpp::grpc_utils
        absl::base
        absl::memory
        absl::status
        absl::variant
        "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>"
    )
endfunction()

link_libraries_to_extension(${EXTENSION_NAME})
link_libraries_to_extension(${LOADABLE_EXTENSION_NAME})

# Install targets
install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
