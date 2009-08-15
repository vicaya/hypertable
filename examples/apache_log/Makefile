# Note, this makefile is for illustration purpose only and probably outdated.
# the actual link line can be obtained from cmake generated makefile
# (use make VERBOSE=1 in build tree to find out)
BOOST_INCLUDE_DIR=/opt/local/include
BOOST_THREAD_LIB=boost_thread-mt
BOOST_IOSTREAMS_LIB=boost_iostreams-mt
BOOST_PROGRAM_OPTIONS_LIB=boost_program_options-mt
BOOST_SYSTEM_LIB=boost_system-mt
SIGAR_LIB=sigar-universal-macosx
HYPERTABLE_INSTALL_DIR=/Users/doug/hypertable/0.9.2.5

CFLAGS=-g -I$(BOOST_INCLUDE_DIR) -I$(HYPERTABLE_INSTALL_DIR)/include
LDFLAGS=-L$(HYPERTABLE_INSTALL_DIR)/lib -lHypertable -lHyperComm -lHyperCommon -lHyperTools -llog4cpp -lexpat -l$(BOOST_THREAD_LIB) -l$(BOOST_IOSTREAMS_LIB) -l$(BOOST_PROGRAM_OPTIONS_LIB) -l$(BOOST_SYSTEM_LIB) -l$(SIGAR_LIB) -lHyperspace -lz -lcurses

all: apache_log_load apache_log_query conf/hypertable.cfg

conf/hypertable.cfg:
	-mkdir conf
	-cp $(HYPERTABLE_INSTALL_DIR)/conf/hypertable.cfg conf

apache_log_load: apache_log_load.o
	g++ -o apache_log_load apache_log_load.o $(LDFLAGS)

apache_log_load.o: apache_log_load.cc
	g++ $(CFLAGS) -c apache_log_load.cc

apache_log_query: apache_log_query.o
	g++ -o apache_log_query apache_log_query.o $(LDFLAGS)

apache_log_query.o: apache_log_query.cc
	g++ $(CFLAGS) -c apache_log_query.cc

clean:
	-rm apache_log_load apache_log_query *.o

