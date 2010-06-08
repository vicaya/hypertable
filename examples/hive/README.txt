This README describes how to run a simple example of the Hive Hypertable integration
For more info on this feature please refer to http://code.google.com/p/hypertable/wiki/HiveIntegration

1. To run this example make sure you have included the path to the thrift jar included with 
Hypertable in your HADOOP_CLASSPATH by editing $HADOOP_HOME/conf/hadoop-env.sh. For, example:
export HADOOP_CLASSPATH=/opt/hypertable/current/lib/java/libthrift-0.3.0.jar:${HADOOP_CLASSPATH}

2. After this, run the Hypertable commands in ht_commands.hql via:
$HT_HOME/bin/ht shell --command-file ht_commands.hql

3. Now run the Hive commands in hive_commands.q via:
$HIVE_HOME/bin/hive --auxpath $HT_HOME/lib/java/hypertable-0.9.3.2.jar,$HT_HOME/lib/java/libthrift-0.3.0.jar -f hive_commands.q 

You can also run the commands in ht_commands.hql and hive_commands.q manually via the 
respective shells command shells.
