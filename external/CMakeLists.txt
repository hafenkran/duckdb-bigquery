cmake_minimum_required(VERSION 3.20)

project(dependencies-build)

# Install and build dependencies locally
include(ExternalProject)
set(LOCAL_INSTALL_DIR ${CMAKE_BINARY_DIR}/local)
set_directory_properties(PROPERTIES EP_BASE ${CMAKE_BINARY_DIR}/projects)
# set(CMAKE_OSX_DEPLOYMENT_TARGET "11.0")

if(${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.24.0")
    cmake_policy(SET CMP0135 NEW)
endif()

ExternalProject_Add(
    google_cloud_cpp_bigquery_rest
    URL ${CMAKE_CURRENT_SOURCE_DIR}/vendor/google-cloud-cpp-2.22.0-fixed.zip
    # CONFIGURE_HANDLED_BY_BUILD TRUE
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${LOCAL_INSTALL_DIR}
               -DCMAKE_PREFIX_PATH=${LOCAL_INSTALL_DIR}
               -DCMAKE_MODULE_PATH=${LOCAL_INSTALL_DIR}/lib/cmake
               -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
               -DCMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}
               -DCMAKE_POSITION_INDEPENDENT_CODE=ON
               -DBUILD_TESTING=OFF
               -DBUILD_SHARED_LIBS=OFF
               # vcpkg options
               -DVCPKG_MANIFEST_DIR=${VCPKG_MANIFEST_DIR}
               -DVCPKG_INSTALLED_DIR=${VCPKG_INSTALLED_DIR}
               -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}
               -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
               # Project Options
               -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF
               -DGOOGLE_CLOUD_CPP_WITH_MOCKS=OFF
               -DGOOGLE_CLOUD_CPP_ENABLE_CCACHE=ON
               -DGOOGLE_CLOUD_CPP_ENABLE=experimental-bigquery_rest)

ExternalProject_Add(
    google_cloud_cpp_bigquery
    URL ${CMAKE_CURRENT_SOURCE_DIR}/vendor/google-cloud-cpp-2.22.0-fixed.zip
    CONFIGURE_HANDLED_BY_BUILD TRUE
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${LOCAL_INSTALL_DIR}
               -DCMAKE_PREFIX_PATH=${LOCAL_INSTALL_DIR}
               -DCMAKE_MODULE_PATH=${LOCAL_INSTALL_DIR}/lib/cmake
               -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
               -DCMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}
               -DCMAKE_POSITION_INDEPENDENT_CODE=ON
               -DBUILD_TESTING=OFF
               -DBUILD_SHARED_LIBS=OFF
               # vcpkg options
               -DVCPKG_MANIFEST_DIR=${VCPKG_MANIFEST_DIR}
               -DVCPKG_INSTALLED_DIR=${VCPKG_INSTALLED_DIR}
               -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
               -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}
               # Project Options
               -DGOOGLE_CLOUD_CPP_ENABLE_EXAMPLES=OFF
               -DGOOGLE_CLOUD_CPP_WITH_MOCKS=OFF
               -DGOOGLE_CLOUD_CPP_ENABLE_CCACHE=ON
               -DGOOGLE_CLOUD_CPP_ENABLE=bigquery)
