set(SOURCES
        geoms.c
        weights.c
        weights_knn.c
        utils.c
        proxy.cpp
        lisa.c
        postgeoda.cpp
        binweight.cpp
        ../../libgeoda_src/gda_weights.cpp
        ../../libgeoda_src/SpatialIndAlgs.cpp
        ../../libgeoda_src/weights/VoronoiUtils.cpp
        ../../libgeoda_src/weights/PolysToContigWeights.cpp
        ../../libgeoda_src/weights/GalWeight.cpp
        ../../libgeoda_src/weights/GeodaWeight.cpp
        ../../libgeoda_src/weights/GwtWeight.cpp
        ../../libgeoda_src/gda_sa.cpp
        ../../libgeoda_src/sa/LISA.cpp
        ../../libgeoda_src/sa/BatchLISA.cpp
        ../../libgeoda_src/sa/BatchLocalMoran.cpp
        ../../libgeoda_src/sa/MultiGeary.cpp
        ../../libgeoda_src/sa/MultiJoinCount.cpp
        ../../libgeoda_src/sa/UniG.cpp
        ../../libgeoda_src/sa/UniGeary.cpp
        ../../libgeoda_src/sa/UniGstar.cpp
        ../../libgeoda_src/sa/UniJoinCount.cpp
        ../../libgeoda_src/sa/UniLocalMoran.cpp
        ../../libgeoda_src/GenGeomAlgs.cpp
        ../../libgeoda_src/GenUtils.cpp
        )

# Add test source code in Debug builds
if (CMAKE_BUILD_TYPE MATCHES Debug)
    set(TS_DEBUG 1)
    set(DEBUG 1)
endif (CMAKE_BUILD_TYPE MATCHES Debug)


set(GEODA_CFLAGS "-D__USE_PTHREAD__")
set(GEODA_CPPFLAGS "-D__USE_PTHREAD__")

# postgres CFLAGS includes -Wdeclaration-after-statement which leads
# to problems when compiling with -Werror since we aim for C99 and allow
# that so we strip this flag from PG_CFLAGS before adding postgres flags
# to our own
string(REPLACE "-Wdeclaration-after-statement" "" PG_CFLAGS "${PG_CFLAGS}")

#set(CMAKE_C_STANDARD 11)
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${PG_LIBDIR}")
set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -L${PG_LIBDIR}")
set(CMAKE_C_FLAGS "${PG_CFLAGS} ${CMAKE_C_FLAGS} ${GEODA_CFLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} ${PG_CPPFLAGS} ${GEODA_CPPFLAGS}")
#set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g")

message(STATUS "CMAKE C FLAGS ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE CXX FLAGS ${CMAKE_CXX_FLAGS}")

# win only
#set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${CMAKE_SOURCE_DIR}/3rd_party/boost_static/lib/${DEP_OS_SUB}/libboost_thread.a")
#set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${CMAKE_SOURCE_DIR}/3rd_party/boost_static/lib/${DEP_OS_SUB}/libboost_system.a")
#set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${CMAKE_SOURCE_DIR}/3rd_party/boost_static/lib/${DEP_OS_SUB}/libboost_thread.a")
#set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${CMAKE_SOURCE_DIR}/3rd_party/boost_static/lib/${DEP_OS_SUB}/libboost_system.a")

if(APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -multiply_defined suppress")
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -multiply_defined suppress -Wl,-undefined,dynamic_lookup -bundle_loader ${PG_BINDIR}/postgres")
endif()

# PG_LDFLAGS can have strange values if not found, so we just add the
# flags if they are defined.
if(PG_LDFLAGS)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${PG_LDFLAGS}")
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${PG_LDFLAGS}")
endif()

include_directories(${PROJECT_SOURCE_DIR}/src #postgeoda
        ${PROJECT_BINARY_DIR}/src # config.h
        ${CMAKE_SOURCE_DIR}/libgeoda_src # libgeoda
        ${CMAKE_SOURCE_DIR}/libgeoda_src/sa # libgeoda
        ${CMAKE_SOURCE_DIR}/libgeoda_src/weights # libgeoda
        )
include_directories(${CMAKE_SOURCE_DIR}/boost)
include_directories(${PG_INCLUDEDIR_SERVER})
include_directories(${PG_INCLUDEDIR})

add_library(${PROJECT_NAME} MODULE ${SOURCES})

#SET_XCODE_PROPERTY(${PROJECT_NAME} CODE_SIGN_IDENTITY "Mac Developer")
#SET_XCODE_PROPERTY(${PROJECT_NAME} DEVELOPMENT_TEAM 7YW2YFK8B4)

set_target_properties(${PROJECT_NAME} PROPERTIES
        OUTPUT_NAME ${PROJECT_NAME}-${PROJECT_VERSION_MOD}
        PREFIX "")

install(
        TARGETS ${PROJECT_NAME}
        DESTINATION ${PG_PKGLIBDIR})

#target_include_directories(${PROJECT_NAME} SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
#target_link_libraries(${PROJECT_NAME} ${OPENSSL_LIBRARIES})

#configure_file(config.h config.h)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})