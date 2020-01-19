set(SOURCES
        geoms.c
        weights.c
        lwgeom_log.c
        )

# Add test source code in Debug builds
if (CMAKE_BUILD_TYPE MATCHES Debug)
    set(TS_DEBUG 1)
    set(DEBUG 1)
endif (CMAKE_BUILD_TYPE MATCHES Debug)


# postgres CFLAGS includes -Wdeclaration-after-statement which leads
# to problems when compiling with -Werror since we aim for C99 and allow
# that so we strip this flag from PG_CFLAGS before adding postgres flags
# to our own
#string(REPLACE "-Wdeclaration-after-statement" "" PG_CFLAGS "${PG_CFLAGS}")

#set(CMAKE_C_STANDARD 11)
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${PG_LIBDIR}")
set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -L${PG_LIBDIR}")
set(CMAKE_C_FLAGS "${PG_CFLAGS} ${CMAKE_C_FLAGS}")
set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} ${PG_CPPFLAGS}")
#set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g")

# PG_LDFLAGS can have strange values if not found, so we just add the
# flags if they are defined.
if(PG_LDFLAGS)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${PG_LDFLAGS}")
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${PG_LDFLAGS}")
endif()

include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PG_INCLUDEDIR_SERVER})
include_directories(${PG_INCLUDEDIR})

add_library(${PROJECT_NAME} MODULE ${SOURCES})

set_target_properties(${PROJECT_NAME} PROPERTIES
        OUTPUT_NAME ${PROJECT_NAME}-${PROJECT_VERSION_MOD}
        PREFIX "")

install(
        TARGETS ${PROJECT_NAME}
        DESTINATION ${PG_PKGLIBDIR})

#target_include_directories(${PROJECT_NAME} SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
#target_link_libraries(${PROJECT_NAME} ${OPENSSL_LIBRARIES})

configure_file(config.h.in config.h)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})