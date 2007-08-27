

CONTENTS

  * HOW TO BUILD
  * HOW TO RUN REGRESSION TESTS


HOW TO BUILD

  1. Install CMake (http://www.cmake.org/)

  2. Make sure Java 1.5 or later and the ant build tool is installed on your machine.

  3. Install the following libraries:
    - Boost version 1.34.1 (http://www.boost.org/)
    - log4cpp version 0.3.5rc3 (http://log4cpp.sourceforge.net/)
    - expat version 2.0.1 (http://sourceforge.net/projects/expat)

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
  


=== HOW TO RUN REGRESSION TESTS ===

1. Make sure software is built and installed according to 'HOW TO BUILD'

   NOTE: These instructions assume the installation directory is ~/hypertable

2. Initialize Hypertable directory layout

  cd ~/hypertable
  ./bin/install

  [Sample output]
  CPU count = 2
  Hypertable.Master.port=38548
  Hypertable.Master.workers=20
  Hypertable.Master.reactors=2
  Successfully initialized.

3. Start the servers using the 'local' DFS

  cd ~/hypertable
  ./bin/start-servers.sh local

4. Run the regression tests

  cd ~/build/hypertable
  make test

  