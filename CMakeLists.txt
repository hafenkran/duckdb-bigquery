cmake_minimum_required(VERSION 3.21.3)

# Set extension name here
set(TARGET_NAME bigquery)
# set(CMAKE_OSX_DEPLOYMENT_TARGET 14.0)

# Compile and install google-cloud-cpp depedencies
message("Installing google-cloud-cpp dependencies")
execute_process(
    COMMAND
        ${CMAKE_COMMAND} -G ${CMAKE_GENERATOR}
        -DDUCKDB_ENABLE_DEPRECATED_API=1
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        -DVCPKG_MANIFEST_DIR=${VCPKG_MANIFEST_DIR}
        -DVCPKG_INSTALLED_DIR=${CMAKE_BINARY_DIR}/vcpkg_installed
        # -DVCPKG_INSTALLED_DIR=${VCPKG_INSTALLED_DIR}
        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
        -DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
        -DCMAKE_OSX_ARCHITECTURES=${DCMAKE_OSX_ARCHITECTURES}
        -DCMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}
        -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}
        -DVCPKG_TARGET_ARCHITECTURE=${VCPKG_TARGET_ARCHITECTURE}
        -S ${CMAKE_CURRENT_SOURCE_DIR}/external -B ${CMAKE_BINARY_DIR}/external
    RESULT_VARIABLE DEPENDENCIES_GENERATE_RESULT)

if(NOT DEPENDENCIES_GENERATE_RESULT EQUAL 0)
    message(FATAL_ERROR "Could not generate dependencies project")
endif()

execute_process(
    COMMAND
        ${CMAKE_COMMAND}
        --build ${CMAKE_BINARY_DIR}/external
        --config ${CMAKE_BUILD_TYPE}
    RESULT_VARIABLE DEPENDENCIES_BUILD_RESULT)

if(NOT DEPENDENCIES_BUILD_RESULT EQUAL 0)
    message(FATAL_ERROR "Could not build dependencies project")
endif()

# Add Dependencies
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${CMAKE_BINARY_DIR}/external/local)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
add_subdirectory(src)

include(${CMAKE_TOOLCHAIN_FILE})

find_package(absl CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(Arrow REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(google_cloud_cpp_bigquery REQUIRED)
find_package(google_cloud_cpp_bigquery_rest REQUIRED)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
#     ${Protobuf_LIBRARIES}
target_link_libraries(${EXTENSION_NAME}
	OpenSSL::SSL
	OpenSSL::Crypto
	google-cloud-cpp::bigquery
    google-cloud-cpp::experimental-bigquery_rest
	"$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")

target_link_libraries(${LOADABLE_EXTENSION_NAME}
	OpenSSL::SSL
	OpenSSL::Crypto
	google-cloud-cpp::bigquery
	google-cloud-cpp::experimental-bigquery_rest
	"$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
