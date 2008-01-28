
To compile this example, you will need to modify the following
variables in the Makefile:

BOOST_INCLUDE_DIR=/usr/local/include/boost-1_34_1
BOOST_THREAD_LIB=boost_thread-mt
HYPERTABLE_INSTALL_DIR=/Users/doug/hypertable

They should be modified to reflect your particular Boost
installation and Hypertable installation.

You can then build the example by just typing 'make'

The servers can be started with the following command
(assuming Hypertable is installed in ~/hypertable):

$ ~/hypertable/bin/start-all-servers.sh local

The next step is to create the 'ApacheWebServer' table
with the following command:

$ ~/hypertable/bin/hypertable --batch < WebServerLog-create-table.hql

There is a compressed sample Apache web log in the file
'access.log.gz'  Uncompress it with:

$ gunzip access.log.gz

You can load it into the table with the following command:

$ ./apache_log_load access.log

You can then query it with a something like the following:

$ ./apache_log_query /favicon.ico
