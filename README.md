Supports:
=========

#org.apache.flume.cassandra.CassandraSearchSink

Can be used to input data into DSE Search (or use cassandra secondary indexes) the Row Key is a UUID. Fields in the event headers are sealized as Column name and value, body of the event is searalized with the column name "data" and the value/body as the byte[].

Property file: cassandra_search.properties

Values supported in cassandra_search.properties

```
read_consistency
write_consistency
max_connections_per_host
seeds
keyspace_name
column_name
timeout_in_ms
```

#org.apache.flume.cassandra.CassandraSink

Can be used to input data into cassandra the row key is the current date (string format: "YYYY-MM-DD HH:MM:SS"). Fields in the headers are sealized as Column name and column value, body of the event is searalized with the column name "data" and the value as the byte[] (event body), optionally users and insert a uniq prefix for all the column names inserted by adding a field name "event_prefix" within the event object. 

Property File: cassandra.properties

Values supported in cassandra.properties

```
read_consistency
write_consistency
max_connections_per_host
seeds
keyspace_name
column_name
timeout_in_ms
```
