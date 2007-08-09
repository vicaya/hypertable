
To Build:

1. Install CMake (http://www.cmake.org/)

2. Make sure Java 1.5 or later is installed on your machine.  Install it if it is not.

  java -version
  java version "1.5.0_07"
  [...]

3. Install the following libraries:
  - Boost version 1.33.1 or later (http://www.boost.org/)
  - log4cpp version 0.3.5rc3 (http://log4cpp.sourceforge.net/)

4. Put the source code somewhere (e.g. ~/code/hypertable/trunk)

5. Build the JAR file

  cd ~/code/hypertable/trunk
  ant jar

6. Create an install directory

  mkdir ~/install

7. Create a build directory

  mkdir -p ~/build/hypertable/trunk

8. Configure the build

  cd ~/build/hypertable/trunk
  cmake -D CMAKE_BUILD_TYPE="Debug" ~/code/hypertable/trunk

9. Build

  cd ~/build/hypertable/trunk
  make
  make install
