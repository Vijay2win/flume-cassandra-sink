package org.apache.flume.cassandra;

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.AstyanaxContext.Builder;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public abstract class AbstractCassandraSink extends AbstractSink implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCassandraSink.class);
    protected AstyanaxContext<Keyspace> context;
    protected Keyspace keyspace;
    protected long timeout;
    protected ColumnFamily<byte[], String> column_family;

    @Override
    public void stop() {
        if (context != null) {
            context.shutdown();
        }
      super.stop();
    }

  @Override
  public void configure(final Context config) {
    String cluster = config.getString("cluster_name", "flume");
    if (keyspace != null) {
      return;
    }

    AstyanaxConfigurationImpl configuration = new AstyanaxConfigurationImpl();
    configuration.setDefaultReadConsistencyLevel(ConsistencyLevel.valueOf(config.getString("read_consistency",
        "CL_ONE")));
    configuration.setDefaultWriteConsistencyLevel(ConsistencyLevel.valueOf(config.getString("write_consistency",
        "CL_ONE")));

    ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl(cluster);
    poolConfig.setMaxConnsPerHost(config.getInteger("max_connections_per_host", 1));
    poolConfig.setSeeds(config.getString("seeds"));

    ConnectionPoolMonitor connectionPoolMonitor = new CountingConnectionPoolMonitor();
    // set this as field for logging purpose only.
    Builder builder = new AstyanaxContext.Builder();
    builder.forCluster(cluster);
    builder.forKeyspace(config.getString("keyspace_name", "flume"));
    builder.withAstyanaxConfiguration(configuration);
    builder.withConnectionPoolConfiguration(poolConfig);
    builder.withConnectionPoolMonitor(connectionPoolMonitor);
    builder.withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

    context = builder.buildKeyspace(ThriftFamilyFactory.getInstance());
    context.start();
    LOG.info("Started keyspace with context: {}", context.toString());
    keyspace = context.getEntity();
    String column_name = config.getString("column_name", "events");
    column_family = new ColumnFamily<byte[], String>(column_name, BytesArraySerializer.get(), StringSerializer.get());

    timeout = config.getLong("timeout_in_ms", 5000L);
  }

    @Override
    public String toString() {
        return "Sink type:" + getClass().getSimpleName();
    }
}
