
================
=== CONTENTS ===
================

  * HOW TO BUILD
  * HOW TO RUN REGRESSION TESTS
  * HOW TO BUILD SOURCE CODE DOCUMENTATION TREE (Doxygen)

====================
=== HOW TO BUILD ===
====================

  1. Install CMake (http://www.cmake.org/)

  2. Install the following libraries:
    - Boost version 1.34.1 (http://www.boost.org/)
    - log4cpp (http://log4cpp.sourceforge.net/)
    - expat (http://sourceforge.net/projects/expat)

    On some GNU/Linux systems, the following packages need to be installed:
    - libattr-devel (sys-apps/attr on Gentoo)
    - readline-devel
    - ncurses-devel

    HIGHLY RECOMMENDED (but not required):
    - tcmalloc (http://code.google.com/p/google-perftools/)

    NOTE: If tcmalloc is available on your system, install it.  Hypertable is
          very malloc intensive and tcmalloc provides a huge performance improvement.

  3. Checkout the source code.

    mkdir ~/src
    cd ~/src
    git clone git://scm.hypertable.org/pub/repos/hypertable.git

  4. Create an install directory

    mkdir ~/hypertable

  5. Create a build directory

    mkdir -p ~/build/hypertable

  6. Configure the build. 

    cd ~/build/hypertable
    cmake -DCMAKE_INSTALL_PREFIX= -DCMAKE_BUILD_TYPE="Debug" ~/src/hypertable

  7. Build the software.

    make
    make install DESTDIR=<your_install_dir>

===================================
=== HOW TO RUN REGRESSION TESTS ===
===================================

1. Make sure software is built and installed according to 'HOW TO BUILD'

2. Make sure you have extended attributes enabled on the partition that holds the
   installation (e.g. ~/hypertable).  This is not necessary on the mac, but is
   in general on Linux systems.  To enable extended attributes on Linux, you need
   to add the user_xattr property to the relevant file systems in your /etc/fstab
   file. For example:

   /dev/hda3     /home     ext3     defaults,user_xattr     1 2

   You can then remount the affected partitions as follows:

   $ mount -o remount /home

3. Restart servers and re-create test tables

   **********************************************************************
   *** WARNING: THIS STEP MUST BE PERFORMED PRIOR TO RUNNING THE TEST ***
   **********************************************************************

  cd ~/hypertable
  bin/kill-servers.sh
  bin/start-all-servers.sh local

4. Run the regression tests

  cd ~/build/hypertable
  make test

=============================================================
=== HOW TO BUILD SOURCE CODE DOCUMENTATION TREE (Doxygen) ===
=============================================================

1. Install the following libraries:
  - doxygen (http://www.stack.nl/~dimitri/doxygen/)
  - graphviz (http://www.graphviz.org/)

2. If you have doxygen installed on your system, then CMake should detect this and
   add a 'doc' target to the make file.  Building the source code documentation
   tree is just a matter of running the following commands:

   cd ~/build/hypertable
   make doc

The documentation tree will get generated under ~/build/hypertable/doc.  To view
the HTML docs, load the following file into a web browser:

  ~/build/hypertable/doc/html/index.html
