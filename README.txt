

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

  8. Configure the build. 

    cd ~/build/hypertable
    cmake -DCMAKE_BUILD_TYPE="Debug" -DCMAKE_INSTALL_PREFIX= ~/code/hypertable

  9. Build the software.

    make
    make install DESTDIR=<your_install_dir>

=== HOW TO RUN REGRESSION TESTS ===

1. Make sure software is built and installed according to 'HOW TO BUILD'

   NOTE: These instructions assume the installation directory is ~/hypertable

2. Stop the servers

  *** Must be performed prior to running the regression tests each time **

  cd ~/hypertable
  ./test/stop-servers.sh

3. Start the servers.  The following example illustrates the use of the 'local'
   DFS broker, but can be changed to use any DFS broker (e.g. hadoop, kosmos, etc.)
   by replacing the 'local' keyword with the name of the broker you want to use.

  *** Must be performed prior to running the regression tests each time **

  cd ~/hypertable
  ./test/start-servers.sh local

  [ Expected output ... ]
  Successfully started DFSBroker (local)
  Successfully started Hyperspace
  Successfully started Hypertable.Master
  Successfully started Hypertable.RangeServer

4. Run the regression tests

  cd ~/build/hypertable
  make test

[ not necessary now, but will be soon ... ]

To set extended attributes, you should add the user_xattr property to the relevant
file systems in your /etc/fstab file. For example:

/dev/hda3     /home     ext3     defaults,user_xattr     1 2

You can then remount the affected partitions as follows:

# mount -o remount /home
