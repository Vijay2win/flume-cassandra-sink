package org.apache.flume.cassandra;

import com.netflix.astyanax.model.ColumnFamily;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class AbstractCassandraSinkTest {

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = "seeds must be defined in context")
  public void testConfigureEmptyContext() throws Exception {
    AbstractCassandraSink sink = new NoOpCassandraSink();
    Context context = new Context();

    sink.configure(context);
  }

  public void testConfigure() throws Exception {
    NoOpCassandraSink sink = new NoOpCassandraSink();
    Context context = new Context();
    context.put("seeds", "localhost");
    sink.configure(context);

    Assert.assertEquals(sink.getChooser().getClass(), DefaultColumnFamilyChooser.class);

    ColumnFamily<byte[], String> columnFamily = sink.getChooser().choose(null);
    Assert.assertEquals(columnFamily.getName(), "events");
  }

  /**
   * Used to test {@link #configure(org.apache.flume.Context)}.
   */
  private static class NoOpCassandraSink extends AbstractCassandraSink {

    @Override
    protected String getConfigName() {
      return null; // this method isn't used anymore
    }

    @Override
    public Status process() throws EventDeliveryException {
      return null;  // no-op
    }

    public ColumnFamilyChooser getChooser() {
      return chooser;
    }
  }
}
