set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
set(VCPKG_OSX_ARCHITECTURES arm64)

# The macOS 27 SDK declares pipe2(), but it is unavailable below the deployment target (11.0).
if(PORT MATCHES "^(c-ares|curl)$")
    list(APPEND VCPKG_CMAKE_CONFIGURE_OPTIONS -DHAVE_PIPE2=OFF)
endif()
