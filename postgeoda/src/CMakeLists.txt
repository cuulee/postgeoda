set(SOURCES
        ../../libgeoda/pg/utils.cpp
        ../../libgeoda/pg/geoms.cpp
        ../../libgeoda/gda_sa.cpp
        ../../libgeoda/gda_data.cpp
        ../../libgeoda/gda_weights.cpp
        ../../libgeoda/gda_clustering.cpp
        ../../libgeoda/GenGeomAlgs.cpp
        ../../libgeoda/GenUtils.cpp
        ../../libgeoda/SpatialIndAlgs.cpp
        ../../libgeoda/weights/VoronoiUtils.cpp
        ../../libgeoda/weights/PolysToContigWeights.cpp
        ../../libgeoda/weights/GalWeight.cpp
        ../../libgeoda/weights/GeodaWeight.cpp
        ../../libgeoda/weights/GwtWeight.cpp
        ../../libgeoda/sa/LISA.cpp
        ../../libgeoda/sa/BatchLISA.cpp
        ../../libgeoda/sa/BatchLocalMoran.cpp
        ../../libgeoda/sa/MultiGeary.cpp
        ../../libgeoda/sa/MultiJoinCount.cpp
        ../../libgeoda/sa/UniG.cpp
        ../../libgeoda/sa/UniGeary.cpp
        ../../libgeoda/sa/UniGstar.cpp
        ../../libgeoda/sa/UniJoinCount.cpp
        ../../libgeoda/sa/UniLocalMoran.cpp
        ../../libgeoda/clustering/redcap.cpp
        ../../libgeoda/clustering/redcap_wrapper.cpp
        ../../libgeoda/clustering/cluster.cpp
        ../../libgeoda/clustering/azp.cpp
        ../../libgeoda/clustering/azp_wrapper.cpp
        ../../libgeoda/clustering/schc_wrapper.cpp
        ../../libgeoda/clustering/fastcluster.cpp
        ../../libgeoda/clustering/maxp_wrapper.cpp
        ../../libgeoda/knn/ANN.cpp
        ../../libgeoda/knn/bd_fix_rad_search.cpp
        ../../libgeoda/knn/bd_pr_search.cpp
        ../../libgeoda/knn/bd_search.cpp
        ../../libgeoda/knn/bd_tree.cpp
        ../../libgeoda/knn/brute.cpp
        ../../libgeoda/knn/kd_dump.cpp
        ../../libgeoda/knn/kd_fix_rad_search.cpp
        ../../libgeoda/knn/kd_pr_search.cpp
        ../../libgeoda/knn/kd_search.cpp
        ../../libgeoda/knn/kd_split.cpp
        ../../libgeoda/knn/kd_tree.cpp
        ../../libgeoda/knn/kd_util.cpp
        ../../libgeoda/knn/perf.cpp
        breaks.c
        skater.c
        weights_cont.c
        weights_knn.c
        weights_dist.c
        localmoran.c
        joincount.c
        localg.c
        quantilelisa.c
        neighbor_match.c
        proxy.cpp
        postgeoda.cpp
        binweight.cpp
        proxy_joincount.cpp
        proxy_localg.cpp
        proxy_quantilelisa.cpp
        proxy_breaks.cpp
        proxy_scc.cpp
        )

# Add test source code in Debug builds
if (CMAKE_BUILD_TYPE MATCHES Debug)
    set(TS_DEBUG 1)
    set(DEBUG 1)
endif (CMAKE_BUILD_TYPE MATCHES Debug)


set(GEODA_CFLAGS "-D__USE_PTHREAD__ -D__PGGEODA__")
set(GEODA_CPPFLAGS "-D__USE_PTHREAD__ -D__PGGEODA__")

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

include_directories(
        ${PROJECT_SOURCE_DIR}/src #postgeoda
        ${CMAKE_SOURCE_DIR} # libgeoda
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