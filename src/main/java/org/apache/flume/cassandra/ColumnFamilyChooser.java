package org.apache.flume.cassandra;

import com.netflix.astyanax.model.ColumnFamily;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

import javax.annotation.Nonnull;

/**
 * Choose a {@link ColumnFamily} to send a given event to.
 */
public interface ColumnFamilyChooser extends Configurable {

  /**
   * Choose a {@link ColumnFamily} to send the given event to.  This method will be called for every event.
   *
   * @param event to index
   * @return column family the event should be sent to
   */
  ColumnFamily<byte[], String> choose(@Nonnull Event event);
}
