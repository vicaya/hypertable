
BOOST_INCLUDE_DIR=/usr/local/include/boost-1_34_1
BOOST_THREAD_LIB=boost_thread-mt
HYPERTABLE_INSTALL_DIR=/Users/doug/hypertable
HYPERTABLE_VERSION=0.9.0

CFLAGS=-g -I$(BOOST_INCLUDE_DIR) -I$(HYPERTABLE_INSTALL_DIR)/include/hypertable-$(HYPERTABLE_VERSION)
LDFLAGS=-L$(HYPERTABLE_INSTALL_DIR)/lib -lHypertable -lHyperComm -lHyperCommon -llog4cpp -lexpat -l$(BOOST_THREAD_LIB) -lHyperspace

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
	-rm *.o *~

