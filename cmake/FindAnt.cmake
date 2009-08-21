# - Find Ant (a java build tool)
# This module defines
#  ANT_VERSION version string of ant if found
#  ANT_FOUND, If false, do not try to use ant

exec_program(ant ARGS -version OUTPUT_VARIABLE ANT_VERSION
             RETURN_VALUE ANT_RETURN)

if (ANT_RETURN STREQUAL "0")
  set(ANT_FOUND TRUE)
  if (NOT ANT_FIND_QUIETLY)
    message(STATUS "Found Ant: ${ANT_VERSION}")
  endif ()
else ()
  set(ANT_FOUND FALSE)
  set(SKIP_JAVA_BUILD TRUE)
endif ()

exec_program(javac ARGS -version OUTPUT_VARIABLE JAVAC_OUT
             RETURN_VALUE JAVAC_RETURN)

if (JAVAC_RETURN STREQUAL "0")
  message(STATUS "    Javac: ${JAVAC_OUT}")
  string(REGEX MATCH "1\\.6\\.*" JAVAC_VERSION ${JAVAC_OUT})

  if (NOT JAVAC_VERSION)
    message(STATUS "    Expected JDK 1.6.*. Skipping Java build")
    set(SKIP_JAVA_BUILD TRUE)
  endif ()
else ()
  message(STATUS "    Javac: not found")
  set(SKIP_JAVA_BUILD TRUE)
endif ()
