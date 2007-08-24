
BUILD INSTRUCTIONS

1. Install CMake (http://www.cmake.org/)

2. Make sure Java 1.5 or later and the ant build tool is installed on your machine.

3. Install the following libraries:
  - Boost version 1.34.1 (http://www.boost.org/)
  - log4cpp version 0.3.5rc3 (http://log4cpp.sourceforge.net/)

4. Checkout the source code.

  mkdir ~/code
  cd ~/code
  svn checkout http://hypertable.googlecode.com/svn/trunk hypertable

5. Build hypertable.jar

  cd ~/code/hypertable
  ant jar

6. Create an install directory

  mkdir ~/hypertable

7. Create a build directory

  mkdir -p ~/build/hypertable

8. Configure the build.  Edit the file ~/code/hypertable/CMakeLists.txt
and modify the following line:

  set (CMAKE_INSTALL_PREFIX "/Users/doug/hypertable")

Change it to point to where you want the binaries to get installed.  Then
configure the build as follows:

  cd ~/build/hypertable
  cmake -D CMAKE_BUILD_TYPE="Debug" ~/code/hypertable

9. Build the software.

  make
  make install


