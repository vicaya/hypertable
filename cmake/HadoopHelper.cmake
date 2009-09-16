# Legacy Hadoop helper

if (JAVA_INCLUDE_PATH)
  message(STATUS "Java headers found at: ${JAVA_INCLUDE_PATH}")
  if (HADOOP_INCLUDE_PATH)
    message(STATUS "Hadoop includes located at: ${HADOOP_INCLUDE_PATH}")
  else ()
    message(STATUS "Please set HADOOP_INCLUDE_PATH variable for Legacy Hadoop support")
  endif(HADOOP_INCLUDE_PATH)

  if (HADOOP_LIB_PATH)
    message(STATUS "Hadoop libraries located at: ${HADOOP_LIB_PATH}")
  else ()
    message(STATUS "Please set HADOOP_LIB_PATH variable for Legacy Hadoop support")
  endif ()

  if (NOT BUILD_SHARED_LIBS)
    message(STATUS "Not building shared libraries. Legacy Hadoop support will be disabled")
  endif ()
else ()
  message(STATUS "Java JNI not found. Legacy Hadoop support will be disabled.")
endif(JAVA_INCLUDE_PATH)
