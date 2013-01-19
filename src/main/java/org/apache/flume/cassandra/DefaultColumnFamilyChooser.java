package org.apache.flume.cassandra;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import org.apache.flume.Context;
import org.apache.flume.Event;

import javax.annotation.Nonnull;

/**
 * {@link #choose(org.apache.flume.Event)} returns the {@link ColumnFamily} defined in the {@link Context}.
 */
public class DefaultColumnFamilyChooser implements ColumnFamilyChooser {
  /**
   * Used by {@link #configure(org.apache.flume.Context)} to look up the column name to use.  This is the key in
   * the {@link Context}.  Defaults to "events".
   */
  public static final String COLUMN_NAME = "column_name";

  private ColumnFamily<byte[], String> columnFamily;

  @Override
  public ColumnFamily<byte[], String> choose(@Nonnull final Event event) {
    return columnFamily;
  }

  @Override
  public void configure(final Context context) {
    final String column_name = context.getString(COLUMN_NAME, "events");
    this.columnFamily = new ColumnFamily<byte[], String>(column_name,
        BytesArraySerializer.get(), StringSerializer.get());
  }
}
