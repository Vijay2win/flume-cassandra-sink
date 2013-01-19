Supports:
=========

#org.apache.flume.cassandra.CassandraSearchSink

Can be used to input data into DSE Search (or use cassandra secondary indexes) the Row Key is a UUID. Fields in the event headers are sealized as Column name and value, body of the event is searalized with the column name "data" and the value/body as the byte[].

Values supported in flume-conf.properties

```
agent.sinks.cassandraEventSink.read_consistency=CL_ONE
agent.sinks.cassandraEventSink.write_consistency=CL_ONE
agent.sinks.cassandraEventSink.max_connections_per_host=10
agent.sinks.cassandraEventSink.seeds=127.0.0.1:9160
agent.sinks.cassandraEventSink.keyspace_name=events
agent.sinks.cassandraEventSink.column_name=event
agent.sinks.cassandraEventSink.timeout_in_ms=5000
agent.sinks.cassandraEventSink.chooser=org.apache.flume.cassandra.DefaultColumnFamilyChooser
```

#org.apache.flume.cassandra.CassandraSink

Can be used to input data into cassandra the row key is the current date (string format: "YYYY-MM-DD HH:MM:SS"). Fields in the headers are sealized as Column name and column value, body of the event is searalized with the column name "data" and the value as the byte[] (event body), optionally users can insert a uniq prefix to all the column names (mentioned above) by adding a field name "event_prefix" within the event object. 

Values supported in flume-conf.properties

```
agent.sinks.cassandraEventSink.read_consistency=CL_ONE
agent.sinks.cassandraEventSink.write_consistency=CL_ONE
agent.sinks.cassandraEventSink.max_connections_per_host=10
agent.sinks.cassandraEventSink.seeds=127.0.0.1:9160
agent.sinks.cassandraEventSink.keyspace_name=events
agent.sinks.cassandraEventSink.column_name=event
agent.sinks.cassandraEventSink.timeout_in_ms=5000
agent.sinks.cassandraEventSink.chooser=org.apache.flume.cassandra.DefaultColumnFamilyChooser
```

#Installation

```
git clone git://github.com/Vijay2win/flume-cassandra-sink.git flume-cassandra-sink
cd flume-cassandra-sink
mvn install
cd target
```

Copy flume* lib/* to the flume installation lib directory.
