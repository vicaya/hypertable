# - Find PYTHON5
# This module defines
#  JAVATHRIFT_FOUND, If false, do not try to use ant

# thrift currently just copies libthrift.jar to /usr/local/lib
set (THRIFT_JAR /usr/local/lib/libthrift.jar)
if (EXISTS ${THRIFT_JAR})
  set(JAVATHRIFT_FOUND TRUE)
  set(JAVAC_FOR_THRIFT "
      <javac srcdir='\${gensrc.dir}' destdir='\${build.classes}'
             deprecation='yes' debug='true'>
        <classpath refid='project_with_thrift.classpath'/>
      </javac>"
    )
  set(JAVAC_CLASSPATH_REFID "project_with_thrift.classpath")
else ()
  set(JAVATHRIFT_FOUND FALSE)
  set(JAVAC_CLASSPATH_REFID "project.classpath")
endif ()

if (JAVATHRIFT_FOUND)
  if (NOT JAVATHRIFT_FIND_QUIETLY)
    message(STATUS "Found thrift for java: ${THRIFT_JAR}")
  endif ()
else ()
  message(STATUS "Thrift for java not found. "
                 "ThriftBroker support for java will be disabled")
endif ()

