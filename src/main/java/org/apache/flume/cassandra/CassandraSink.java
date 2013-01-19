package org.apache.flume.cassandra;

import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.serializers.StringSerializer;

public class CassandraSink extends AbstractCassandraSink {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraSink.class);
    private static final String EVENT_PREFIX_KEY = "event_prefix";

    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event event = channel.take();
            if (event == null) {
                transaction.commit();
                return Status.BACKOFF;
            }
            MutationBatch mutation = keyspace.prepareMutationBatch();
            long timestamp = System.currentTimeMillis();
            String date = getDate(timestamp);
            ColumnListMutation<String> columns = mutation.withRow(chooser.choose(event), StringSerializer.get().toBytes(date));
            String prefix = null;
            if (event.getHeaders() != null) {
                prefix = event.getHeaders().get(EVENT_PREFIX_KEY);
                for (Entry<String, String> entry : event.getHeaders().entrySet())
                    columns.putColumn((prefix == null) ? entry.getKey() : (prefix + ":" + entry.getKey()),
                            entry.getValue());
            }
            columns.putColumn((prefix == null) ? "data" : (prefix + ":data"), event.getBody());
            Future<OperationResult<Void>> future = mutation.executeAsync();
            future.get(timeout, TimeUnit.MILLISECONDS);
            transaction.commit();
            return Status.READY;
        } catch (Exception eIO) {
            transaction.rollback();
            LOG.warn("Error writing to Cassandra: ", eIO);
            return Status.BACKOFF;
        } catch (Throwable th) {
            transaction.rollback();
            LOG.error("process failed", th);
            throw new RuntimeException(th);
        } finally {
            transaction.close();
        }
    }

    /**
     * Format YYYY-MM-DD HH:MM:SS
     */
    private String getDate(long current) {
        DateTime time = new DateTime(current);
        return time.toString("YYYY-MM-DD HH:MM:SS");
    }

    @Override
    protected String getConfigName() {
        return "cassandra.properties";
    }
}
