# Obtain the soname via objdump
macro(HT_GET_SONAME var fpath)
  exec_program(${CMAKE_SOURCE_DIR}/bin/soname.sh ARGS ${fpath}
               OUTPUT_VARIABLE SONAME_OUT RETURN_VALUE SONAME_RETURN)
  set(${var})

  if (SONAME_RETURN STREQUAL "0")
    set(${var} ${SONAME_OUT})
  endif ()

  if (NOT ${var})
    get_filename_component(${var} ${fpath} NAME)
  endif ()

  if (HT_CMAKE_DEBUG)
    message("SONAME: ${fpath} -> ${${var}}")
  endif ()

  # check if the library is prelinked, if so, warn
  exec_program(objdump ARGS -h ${fpath} OUTPUT_VARIABLE ODH_OUT
               RETURN_VALUE ODH_RETURN)
  if (ODH_RETURN STREQUAL "0")
    string(REGEX MATCH "prelink_undo" match ${ODH_OUT})
    if (match)
      message("WARNING: ${fpath} is prelinked, RPMs may require --nomd5")
    endif ()
  endif ()
endmacro()

# This is a workaround for install() which always preserves symlinks
macro(HT_INSTALL_COPY dest)
  foreach(fpath ${ARGN})
    if (NOT ${fpath} MATCHES "(NOTFOUND|\\.a)$")
      #message(STATUS "install copy: ${fpath}")
      HT_GET_SONAME(soname ${fpath})
      configure_file(${fpath} "${dest}/${soname}" COPYONLY)
      install(FILES "${CMAKE_BINARY_DIR}/${dest}/${soname}" DESTINATION ${dest})
    endif ()
  endforeach()
endmacro()

# Dependent libraries
HT_INSTALL_COPY(lib ${BOOST_LIBS} ${BDB_LIBRARIES} ${Thrift_LIBS}
                ${Kfs_LIBRARIES} ${LibEvent_LIB} ${Log4cpp_LIBRARIES}
                ${READLINE_LIBRARIES} ${EXPAT_LIBRARIES} ${BZIP2_LIBRARIES}
                ${ZLIB_LIBRARIES} ${SIGAR_LIBRARY} ${Tcmalloc_LIBRARIES})

# Need to include some "system" libraries as well
exec_program(${CMAKE_SOURCE_DIR}/bin/ldd.sh
             ARGS ${CMAKE_BINARY_DIR}/CMakeFiles/CompilerIdCXX/a.out
             OUTPUT_VARIABLE LDD_OUT RETURN_VALUE LDD_RETURN)

if (HT_CMAKE_DEBUG)
  message("ldd.sh output: ${LDD_OUT}")
endif ()

if (LDD_RETURN STREQUAL "0")
  string(REGEX MATCH " (/[^ ]+/libgcc_s\\.[^ ]+)" dummy ${LDD_OUT})
  set(gcc_s_lib ${CMAKE_MATCH_1})
  string(REGEX MATCH " (/[^ ]+/libstdc\\+\\+\\.[^ ]+)" dummy ${LDD_OUT})
  set(stdcxx_lib ${CMAKE_MATCH_1})
  HT_INSTALL_COPY(lib ${gcc_s_lib} ${stdcxx_lib})
endif ()

# General package variables
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
  set(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_SOURCE_DIR}/doc/PACKAGE.txt")
endif ()

if (NOT CPACK_RESOURCE_FILE_LICENSE)
  set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE.txt")
endif ()

set(CPACK_PACKAGE_VERSION ${VERSION})
set(CPACK_PACKAGE_VERSION_MAJOR ${VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH "${VERSION_MICRO}.${VERSION_PATCH}")
set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CMAKE_INSTALL_PREFIX})

# packaging expecting i386 instead of i686 etc.
string(REGEX REPLACE "i[3-6]86" i386 MACH ${CMAKE_SYSTEM_PROCESSOR})

string(TOLOWER "${CPACK_PACKAGE_NAME}-${VERSION}-${CMAKE_SYSTEM_NAME}-${MACH}"
       CPACK_PACKAGE_FILE_NAME)

if (NOT CMAKE_BUILD_TYPE STREQUAL "Release")
  string(TOLOWER "${CPACK_PACKAGE_FILE_NAME}-${CMAKE_BUILD_TYPE}"
         CPACK_PACKAGE_FILE_NAME)
endif ()

set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

# Debian pakcage variables
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    "${CMAKE_BINARY_DIR}/postinst;${CMAKE_BINARY_DIR}/prerm")

# RPM package variables
# rpm perl dependencies stuff is dumb
set(CPACK_RPM_SPEC_MORE_DEFINE "
Provides: perl(Hypertable::ThriftGen2::HqlService)
Provides: perl(Hypertable::ThriftGen2::Types)
Provides: perl(Hypertable::ThriftGen::ClientService)
Provides: perl(Hypertable::ThriftGen::Types)
Provides: perl(Thrift)
Provides: perl(Thrift::BinaryProtocol)
Provides: perl(Thrift::FramedTransport)
Provides: perl(Thrift::Socket)

%post
${CMAKE_INSTALL_PREFIX}/bin/fhsize.sh

%preun
${CMAKE_INSTALL_PREFIX}/bin/prerm.sh")

include(CPack)
