# Obtain the soname via objdump
macro(HT_GET_SONAME var fpath)
  exec_program(objdump ARGS -p ${fpath} OUTPUT_VARIABLE OD_OUT
               RETURN_VALUE OD_RETURN)
  set(${var})

  if (OD_RETURN STREQUAL "0")
    string(REGEX MATCH "SONAME +([^ \r\n]+)" dummy ${OD_OUT})
    set(${var} ${CMAKE_MATCH_1})
  endif ()

  if (NOT ${var})
    get_filename_component(${var} ${fpath} NAME)
  endif ()
  #message("${fpath} -> ${${var}}")
endmacro()

# This is a workaround for install() which always preserves symlinks
macro(HT_INSTALL_COPY dest)
  foreach(fpath ${ARGN})
    if (NOT ${fpath} MATCHES "NOTFOUND$")
      #message(STATUS "install copy: ${fpath}")
      HT_GET_SONAME(soname ${fpath})
      configure_file(${fpath} "${dest}/${soname}" COPYONLY)
      install(FILES "${CMAKE_BINARY_DIR}/${dest}/${soname}" DESTINATION ${dest})
    endif ()
  endforeach()
endmacro()

# Dependent libraries
HT_INSTALL_COPY(lib ${BOOST_LIBS} ${BDB_LIBRARIES} ${THRIFT_JAR} ${Thrift_LIBS}
                ${Kfs_LIBRARIES} ${LibEvent_LIB} ${Log4cpp_LIBRARIES}
                ${READLINE_LIBRARIES} ${EXPAT_LIBRARIES} ${BZIP2_LIBRARIES}
                ${ZLIB_LIBRARIES} ${SIGAR_LIBRARY} ${Tcmalloc_LIBRARIES})

# Need to include some "system" libraries as well
exec_program(ldd ARGS ${CMAKE_BINARY_DIR}/CMakeFiles/CompilerIdCXX/a.out
             OUTPUT_VARIABLE LDD_OUT RETURN_VALUE LDD_RETURN)

if (LDD_RETURN STREQUAL "0")
  string(REGEX MATCH "=> (/[^ ]+/libgcc_s.so[^ ]+)" dummy ${LDD_OUT})
  set(gcc_s_lib ${CMAKE_MATCH_1})
  string(REGEX MATCH "=> (/[^ ]+/libstdc...so[^ ]+)" dummy ${LDD_OUT})
  set(stdcxx_lib ${CMAKE_MATCH_1})
  HT_INSTALL_COPY(lib ${gcc_s_lib} ${stdcxx_lib})
endif ()

if (NOT CPACK_PACKAGE_NAME)
  set(CPACK_PACKAGE_NAME "hypertable")
endif ()

if (NOT CPACK_PACKAGE_CONTACT)
  set(CPACK_PACKAGE_CONTACT "llu@hypertable.org")
endif ()

if (NOT CPACK_PACKAGE_DESCRIPTION_SUMMARY)
  set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Hypertable ${VERSION}")
endif ()

if (NOT CPACK_PACKAGE_VENDOR)
  set(CPACK_PACKAGE_VENDOR "hypertable.org")
endif ()

if (NOT CPACK_PACKAGE_DESCRIPTION_FILE)
  set(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_SOURCE_DIR}/START.markdown")
endif ()

if (NOT CPACK_RESOURCE_FILE_LICENSE)
  set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")
endif ()

set(CPACK_PACKAGE_VERSION ${VERSION})
set(CPACK_PACKAGE_VERSION_MAJOR ${VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH "${VERSION_MICRO}.${VERSION_PATCH}")
set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CMAKE_INSTALL_PREFIX})

string(TOLOWER "${CPACK_PACKAGE_NAME}-${VERSION}-${CMAKE_SYSTEM_NAME}-${CMAKE_SYSTEM_PROCESSOR}" CPACK_PACKAGE_FILE_NAME)
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    "${CMAKE_BINARY_DIR}/postinst;${CMAKE_BINARY_DIR}/postrm")

# rpm perl dependencies stuff is dumb
set(CPACK_RPM_SPEC_MORE_DEFINE
    "provides: perl(Hypertable::ThriftGen2::HqlService)
     provides: perl(Hypertable::ThriftGen2::Types)
     provides: perl(Hypertable::ThriftGen::ClientService)
     provides: perl(Hypertable::ThriftGen::Types)
     provides: perl(Thrift)
     provides: perl(Thrift::BinaryProtocol)
     provides: perl(Thrift::FramedTransport)
     provides: perl(Thrift::Socket)")

include(CPack)
