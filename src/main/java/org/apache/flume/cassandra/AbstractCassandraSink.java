package org.apache.flume.cassandra;

import java.io.FileReader;
import java.net.URL;
import java.util.Properties;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.flume.sink.AbstractSink;
import org.mortbay.log.Log;
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

public abstract class AbstractCassandraSink extends AbstractSink {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCassandraSink.class);
    protected AstyanaxContext<Keyspace> context;
    protected Keyspace keyspace;
    protected long timeout;
    protected ColumnFamily<byte[], String> column_family;

    @Override
    public void stop() {
        if (context != null)
            context.shutdown();
    }

    public void intialize(Properties config) {
        String cluster = config.getProperty("cluster_name", "flume");
        if (keyspace != null)
            return;

        AstyanaxConfigurationImpl configuration = new AstyanaxConfigurationImpl();
        configuration.setDefaultReadConsistencyLevel(ConsistencyLevel.valueOf(config.getProperty("read_consistency",
                "CL_ONE")));
        configuration.setDefaultWriteConsistencyLevel(ConsistencyLevel.valueOf(config.getProperty("write_consistency",
                "CL_ONE")));

        String maxConnection = config.getProperty("max_connections_per_host");
        ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl(cluster);
        poolConfig.setMaxConnsPerHost(Integer.parseInt(maxConnection));
        poolConfig.setSeeds(config.getProperty("seeds"));

        ConnectionPoolMonitor connectionPoolMonitor = new CountingConnectionPoolMonitor();
        // set this as field for logging purpose only.
        Builder builder = new AstyanaxContext.Builder();
        builder.forCluster(cluster);
        builder.forKeyspace(config.getProperty("keyspace_name", "flume"));
        builder.withAstyanaxConfiguration(configuration);
        builder.withConnectionPoolConfiguration(poolConfig);
        builder.withConnectionPoolMonitor(connectionPoolMonitor);
        builder.withConnectionPoolMonitor(new CountingConnectionPoolMonitor());

        context = builder.buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        Log.info("Started keyspace with context: %s", context.toString());
        keyspace = context.getEntity();
        String column_name = config.getProperty("column_name", "events");
        column_family = new ColumnFamily<byte[], String>(column_name, BytesArraySerializer.get(), StringSerializer.get());

        timeout = Long.parseLong(config.getProperty("timeout_in_ms", "5000"));
    }

    @Override
    public void start() {
        FileReader reader = null;
        try {
            URL resource = this.getClass().getClassLoader().getResource(getConfigName());
            reader = new FileReader(resource.getFile());
            Properties config = new Properties();
            config.load(reader);
            intialize(config);
        } catch (Exception e) {
            LOG.error("Error in intializing the plugin", e);
            throw new RuntimeException(e);
        } finally {
            FileUtils.closeQuietly(reader);
        }
    }

    protected abstract String getConfigName();

    @Override
    public String toString() {
        return "Sink type:" + getClass().getSimpleName();
    }
}
