cmake_minimum_required(VERSION 3.2)

project(geoda)

###########################################################
#
# env variables
#
###########################################################

message(STATUS "Binary dir: ${CMAKE_BINARY_DIR}")

set( CMAKE_VERBOSE_MAKEFILE on )

set (DEP_LIBRARY_PATH ${CMAKE_SOURCE_DIR}/3rd_party)
set (DEP_OS_SUB "")

if ( APPLE )
    set(CMAKE_MACOSX_RPATH 1)
    set (DEP_OS_SUB "osx")

    # on macOS "uname -m" returns the architecture (x86_64 or arm64)
    execute_process(
            COMMAND uname -m
            RESULT_VARIABLE result
            OUTPUT_VARIABLE OSX_NATIVE_ARCHITECTURE
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )

    # determine if we do universal build or native build
    if(OSX_NATIVE_ARCHITECTURE STREQUAL "arm64") # when running on arm64
        set (CMAKE_OSX_ARCHITECTURES "arm64")
        set (CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -arch arm64")
        message(STATUS "macOS arm64 build")
    else()
        set (CMAKE_OSX_ARCHITECTURES "x86_64")
        set (CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -arch x86_64")
        message(STATUS "macOS x86_64 build")
    endif()


elseif (UNIX)
    set (DEP_OS_SUB "linux")
    set (CMAKE_CXX_FLAGS  "-fPIC -Wl,--no-as-needed")

elseif (MINGW)
    # i386 or x64
    set (DEP_OS_SUB "i386")

elseif (MSVC)
    # win32 or win64
    set (DEP_OS_SUB "win32")
    # if win64: 
    #set(CMAKE_GENERATOR_PLATFORM x64)
endif()

message(STATUS "DEP_OS_SUB: ${DEP_OS_SUB}")

# This little macro lets you set any XCode specific property
macro (set_xcode_property TARGET XCODE_PROPERTY XCODE_VALUE)
    set_property (TARGET ${TARGET} PROPERTY XCODE_ATTRIBUTE_${XCODE_PROPERTY} ${XCODE_VALUE})
endmacro (set_xcode_property)

add_subdirectory(postgeoda)

