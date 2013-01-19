package org.apache.flume.cassandra;

import com.netflix.astyanax.model.ColumnFamily;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class DefaultColumnFamilyChooserTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultColumnFamilyChooserTest.class.getName());

  public void testConfigure() throws Exception {
    ColumnFamilyChooser chooser = new DefaultColumnFamilyChooser();
    Context context = new Context();
    context.put(DefaultColumnFamilyChooser.COLUMN_NAME, "testConfigure");
    chooser.configure(context);

    ColumnFamily<byte[], String> cf = chooser.choose(null);
    Assert.assertEquals(cf.getName(), "testConfigure");

    context.put(DefaultColumnFamilyChooser.COLUMN_NAME, "testConfigure-v2");
    chooser.configure(context);

    cf = chooser.choose(null);
    Assert.assertEquals(cf.getName(), "testConfigure-v2");
  }
}
