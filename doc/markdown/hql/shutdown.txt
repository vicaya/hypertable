SHUTDOWN
--------
#### EBNF

    SHUTDOWN

#### Description
<p>
The `SHUTDOWN` command gracefully shuts down the range servers by invoking the
`RangeServer::shutdown()` command on each server.  The `RangeServer::shutdown()`
command performs the following sequence of steps:

  * close the RSML (range transaction log)
  * close the ROOT commit log
  * close the METADATA commit log
  * close the USER commit log

Hypertable must be shut down with this command when running on a filesystem
such as HDFS 0.20 since it doesn't have a properly functioning `fsync()`
operation.  Any other method of shutting down the range servers could result
in loss of data, or worse, inconsistent or corrupt system state.
