add_library(ht HypertableBindings.cc)

include_directories(${PYTHON_INCLUDE_PATH} ${Boost_INCLUDE_DIRS} "../../../src/cc")
link_directories(${Boost_LIBRARIES})

target_link_libraries(ht ${BOOST_PYTHON_LIB} Hypertable)

