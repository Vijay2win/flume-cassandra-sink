package org.apache.flume.cassandra;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import org.apache.flume.Context;
import org.apache.flume.Event;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * Uses {@link Event} headers to determine the {@link ColumnFamily} to return.  If multiple headers are used then the
 * column family name will be the concatenation of the event headers with a join string (defaults to "_").  If the event
 * does not have any of the headers defined then a {@link IllegalStateException} will be thrown.  If some of the event
 * (but not all) are missing, then the name will be the concatenation of the header values present.
 */
public class HeaderColumnFamilyChooser implements ColumnFamilyChooser {
  /**
   * Used by {@link #configure(org.apache.flume.Context)} to define which string should be used while joining header
   * values.
   */
  public static final String JOINER_KEY = "chooser.join";
  /**
   * Used by {@link #configure(org.apache.flume.Context)} to define which headers to check for.
   */
  public static final String HEADERS_KEY = "chooser.headers";

  private static final Splitter HEADER_KEY_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

  private Joiner joiner;
  private Iterable<String> headersKeys;

  @Override
  public ColumnFamily<byte[], String> choose(@Nonnull final Event event) {
    final Map<String, String> headers = event.getHeaders();
    final List<String> values = Lists.newArrayList();
    for (final String key : headersKeys) {
      values.add(headers.get(key));
    }
    final String column_name = joiner.join(values);
    if (Strings.isNullOrEmpty(column_name)) {
      throw new IllegalStateException("Event " + event + " doesn't have any of the headers defined: "
          + Iterables.toString(headersKeys));
    }
    return new ColumnFamily<byte[], String>(column_name, BytesArraySerializer.get(), StringSerializer.get());
  }

  @Override
  public void configure(final Context context) {
    Preconditions.checkArgument(context.getString(HEADERS_KEY) != null, "chooser.headers must be defined");

    joiner = Joiner.on(context.getString(JOINER_KEY, "_")).skipNulls();
    headersKeys = HEADER_KEY_SPLITTER.split(context.getString("chooser.headers"));
  }
}
