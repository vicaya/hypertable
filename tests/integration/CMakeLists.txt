# CMake file for integration tests

add_subdirectory(random)
add_subdirectory(commit-log-gc)
add_subdirectory(load-exception-after-split)
add_subdirectory(metadata-split)
add_subdirectory(multiple-maintenance-threads)
add_subdirectory(row-overflow)
add_subdirectory(rowkey-ag-imbalance)
#add_subdirectory(scan-concurrency)
add_subdirectory(split-recovery)
add_subdirectory(bloomfilter)
add_subdirectory(scan-limit)
