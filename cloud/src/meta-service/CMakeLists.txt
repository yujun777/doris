

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/meta-service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/meta-service")

# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lfdb_c -L${THIRDPARTY_DIR}/lib")

add_library(MetaService
    meta_server.cpp
    meta_service.cpp
    meta_service_http.cpp
    injection_point_http.cpp
    meta_service_job.cpp
    meta_service_resource.cpp
    meta_service_schema.cpp
    meta_service_snapshot.cpp
    meta_service_tablet_stats.cpp
    meta_service_partition.cpp
    meta_service_txn.cpp
    meta_service_util.cpp
    doris_txn.cpp
    http_encode_key.cpp
    txn_lazy_committer.cpp
    delete_bitmap_lock_white_list.cpp
)
