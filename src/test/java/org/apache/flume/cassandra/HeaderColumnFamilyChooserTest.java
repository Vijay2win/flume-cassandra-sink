package org.apache.flume.cassandra;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.model.ColumnFamily;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class HeaderColumnFamilyChooserTest {

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = "chooser.headers must be defined")
  public void emptyContext() {
    Context context = new Context();
    ColumnFamilyChooser chooser = new HeaderColumnFamilyChooser();
    chooser.configure(context);
  }

  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*doesn't have any of the headers defined.*")
  public void noHeadersInEvent() {
    Context context = new Context();
    context.put(HeaderColumnFamilyChooser.HEADERS_KEY, "noHeaders");
    ColumnFamilyChooser chooser = new HeaderColumnFamilyChooser();
    chooser.configure(context);

    Event event = EventBuilder.withBody("body", Charsets.UTF_8);
    ColumnFamily<byte[], String> family = chooser.choose(event);
  }

  public void singleHeader() {
    Context context = new Context();
    context.put(HeaderColumnFamilyChooser.HEADERS_KEY, "singleHeader");
    ColumnFamilyChooser chooser = new HeaderColumnFamilyChooser();
    chooser.configure(context);

    Event event = EventBuilder.withBody("body", Charsets.UTF_8, ImmutableMap.of("singleHeader", "foo"));
    ColumnFamily<byte[], String> family = chooser.choose(event);
    Assert.assertEquals(family.getName(), "foo");
  }

  public void multiHeaders() {
    Context context = new Context();
    context.put(HeaderColumnFamilyChooser.HEADERS_KEY, "a,b, c");
    context.put(HeaderColumnFamilyChooser.JOINER_KEY, ".");
    ColumnFamilyChooser chooser = new HeaderColumnFamilyChooser();
    chooser.configure(context);

    Event event = EventBuilder.withBody("body", Charsets.UTF_8, ImmutableMap.of("a", "foo", "b", "bar", "c", "baz"));
    ColumnFamily<byte[], String> family = chooser.choose(event);
    Assert.assertEquals(family.getName(), "foo.bar.baz");
  }

  public void multiHeadersEventsLacking() {
    Context context = new Context();
    context.put(HeaderColumnFamilyChooser.HEADERS_KEY, "a,b, c");
    context.put(HeaderColumnFamilyChooser.JOINER_KEY, "#");
    ColumnFamilyChooser chooser = new HeaderColumnFamilyChooser();
    chooser.configure(context);

    Event event = EventBuilder.withBody("body", Charsets.UTF_8, ImmutableMap.of("a", "foo", "c", "baz"));
    ColumnFamily<byte[], String> family = chooser.choose(event);
    Assert.assertEquals(family.getName(), "foo#baz");
  }
}
