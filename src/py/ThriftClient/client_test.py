from hypertable.thriftclient import *

client = ThriftClient("localhost", 38080)
res = client.hql_query("show tables")
print res
res = client.hql_query("select * from thrift_test")
print res
