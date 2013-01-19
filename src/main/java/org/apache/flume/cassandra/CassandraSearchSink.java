package org.apache.flume.cassandra;

import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;

public class CassandraSearchSink extends AbstractCassandraSink {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraSearchSink.class);

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
            ColumnListMutation<String> columns = mutation.withRow(chooser.choose(event),
              TimeUUIDSerializer.get().toBytes(UUID.randomUUID()));
            if (event.getHeaders() != null)
                for (Entry<String, String> entry : event.getHeaders().entrySet())
                    columns.putColumn(entry.getKey(), entry.getValue());
            columns.putColumn("data", event.getBody());

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

    @Override
    protected String getConfigName() {
        return "cassandra_search.properties";
    }
}
