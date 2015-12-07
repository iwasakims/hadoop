/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.metrics2.sink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.MetricsVisitor;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.impl.ConfigBuilder;
import org.apache.hadoop.metrics2.impl.MsInfo;
import org.apache.hadoop.metrics2.sink.InfluxDBSink;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.junit.Ignore;
import org.junit.Test;

public class TestInfluxDBSink {
  private static final Log LOG = LogFactory.getLog(TestInfluxDBSink.class);

  @Test
  public void testBuildLine() {
    MetricsRecord record = getTestRecord(10000L, 12345L);
    assertEquals(
        "test.name1,Context=test,Hostname=host1,ProcessName=process\\ name" +
        " metric1=12345,metric2=2.0 10000000000\n",
        InfluxDBSink.buildLine(new StringBuilder(), record).toString());
  }

  @Test
  public void testDefaultConfiguration() throws IOException {
    ConfigBuilder cb = new ConfigBuilder();
    InfluxDBSink.InfluxDB  influxdb =
        InfluxDBSink.getInfluxDB(cb.subset("test.sink.influxdb"));
    assertTrue("instance must be HttpInfluxDB",
        influxdb instanceof InfluxDBSink.HttpInfluxDB);
    assertEquals("http://localhost:8086/write?db=mydb",
        ((InfluxDBSink.HttpInfluxDB) influxdb).getURI());
  }

  @Test
  public void testHttpConfiguration() throws IOException {
    ConfigBuilder cb = new ConfigBuilder()
        .add("test.sink.influxdb.servers", "host1:1234")
        .add("test.sink.influxdb.db", "db1");
    InfluxDBSink.InfluxDB  influxdb =
        InfluxDBSink.getInfluxDB(cb.subset("test.sink.influxdb"));
    assertTrue("instance must be HttpInfluxDB",
        influxdb instanceof InfluxDBSink.HttpInfluxDB);
    assertEquals("http://host1:1234/write?db=db1",
        ((InfluxDBSink.HttpInfluxDB) influxdb).getURI());
  }

  @Test
  public void testHttpParams() throws IOException {
    ConfigBuilder cb = new ConfigBuilder()
        .add("test.sink.influxdb.protocol", "http")
        .add("test.sink.influxdb.connection-timeout", "30000")
        .add("test.sink.influxdb.sotimeout", "60000");
    InfluxDBSink.InfluxDB  influxdb =
        InfluxDBSink.getInfluxDB(cb.subset("test.sink.influxdb"));
    assertTrue("instance must be HttpInfluxDB",
        influxdb instanceof InfluxDBSink.HttpInfluxDB);
    HttpParams params =
        ((InfluxDBSink.HttpInfluxDB) influxdb).getClient().getParams();
    assertEquals(30000, HttpConnectionParams.getConnectionTimeout(params));
    assertEquals(60000, HttpConnectionParams.getSoTimeout(params));
  }

  @Test
  public void testUdpConfiguration() throws IOException {
    ConfigBuilder cb = new ConfigBuilder()
        .add("test.sink.influxdb.protocol", "udp");
    InfluxDBSink.InfluxDB  influxdb =
        InfluxDBSink.getInfluxDB(cb.subset("test.sink.influxdb"));
    assertTrue("instance must be UdpInfluxDB",
        influxdb instanceof InfluxDBSink.UdpInfluxDB);
  }

  @Test
  public void testInvalidProtocolConfiguration() throws IOException {
    ConfigBuilder cb = new ConfigBuilder()
        .add("test.sink.influxdb.protocol", "invlid");
    try {
      InfluxDBSink.InfluxDB  influxdb =
          InfluxDBSink.getInfluxDB(cb.subset("test.sink.influxdb"));
      fail("should throw exception for invalid configuration.");
    } catch (IOException e) {
      // should throw IOException.
    }
  }

  @Test
  public void testInvalidHttpServersConfiguration() throws IOException {
    ConfigBuilder cb = new ConfigBuilder()
        .add("test.sink.influxdb.protocol", "http")
        .add("test.sink.influxdb.servers", "---invalid---");
    try {
      InfluxDBSink.InfluxDB  influxdb =
          InfluxDBSink.getInfluxDB(cb.subset("test.sink.influxdb"));
      fail("should throw exception for invalid servers configuration.");
    } catch (IllegalArgumentException e) {
      // should throw IllegalArgumentException.
    }
  }

  @Test
  public void testInvalidUdpServersConfiguration() throws IOException {
    ConfigBuilder cb = new ConfigBuilder()
        .add("test.sink.influxdb.protocol", "udp")
        .add("test.sink.influxdb.servers", "---invalid,servers---");
    try {
      InfluxDBSink.InfluxDB  influxdb =
          InfluxDBSink.getInfluxDB(cb.subset("test.sink.influxdb"));
      fail("should throw exception for invalid servers configuration.");
    } catch (IllegalArgumentException e) {
      // should throw IllegalArgumentException.
    }
  }

  @Test
  public void testInitFailure1() throws IOException {
    ConfigBuilder cb = new ConfigBuilder()
        .add("test.sink.influxdb.protocol", "invlid");
    InfluxDBSink sink = new InfluxDBSink();

    // should not throw exception
    sink.init(cb.subset("test.sink.influxdb"));

    // should be no-op
    sink.putMetrics(null);
  }
  
  @Test
  public void testInitFailure2() throws IOException {
    ConfigBuilder cb = new ConfigBuilder()
        .add("test.sink.influxdb.servers", "---invlid---");
    InfluxDBSink sink = new InfluxDBSink();

    // should not throw exception
    sink.init(cb.subset("test.sink.influxdb"));

    // should be no-op
    sink.putMetrics(null);
  }
  
  @Ignore
  @Test
  public void testHttpInflnuxDBSink() throws Exception {
    main(new String[]{"localhost:8086", "http", "mydb"});
  }

  @Ignore
  @Test
  public void testUdpInfluxDBSink() throws Exception {
    main(new String[]{"localhost:8087", "udp"});
  }

  private static MetricsRecord getTestRecord(long timestamp, long metric1) {
    List<MetricsTag> tags = new ArrayList<MetricsTag>();
    tags.add(new MetricsTag(MsInfo.Context, "test"));
    tags.add(new MetricsTag(MsInfo.Hostname, "host1"));
    tags.add(new MetricsTag(MsInfo.ProcessName, "process name"));
    List<AbstractMetric> metrics = new ArrayList<AbstractMetric>();
    metrics.add(new TestMetric("metric1", metric1));
    metrics.add(new TestMetric("metric2", 2.0));
    MetricsInfo info = new TestMetricsInfo("name1");
    return new TestMetricsRecord(info, (long) timestamp, tags, metrics);
  }

  private static class TestMetricsInfo implements MetricsInfo {
    private final String name;

    public TestMetricsInfo(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String description() {
      return "metrics description.";
    }
  }

  private static class TestMetric extends AbstractMetric {
    private final Number value;

    public TestMetric(String name, Number value) {
      super(new TestMetricsInfo(name));
      this.value = value;
    }

    @Override
    public Number value() {
      return value;
    }

    @Override
    public MetricType type() {
      return MetricType.COUNTER;
    }

    @Override
    public void visit(MetricsVisitor visitor) {
    };
  }

  private static class TestMetricsRecord implements MetricsRecord {
    private final long timestamp;
    private final MetricsInfo info;
    private final List<MetricsTag> tags;
    private final Iterable<AbstractMetric> metrics;

    public TestMetricsRecord(MetricsInfo info, long timestamp,
        List<MetricsTag> tags, Iterable<AbstractMetric> metrics) {
      this.timestamp = timestamp;
      this.info = info;
      this.tags = tags;
      this.metrics = metrics;
    }

    @Override
    public long timestamp() {
      return timestamp;
    }

    @Override
    public String name() {
      return info.name();
    }

    @Override
    public String description() {
      return info.description();
    }

    @Override
    public String context() {
      for (MetricsTag t : tags) {
        if (t.info() == MsInfo.Context) {
          return t.value();
        }
      }
      return "default";
    }

    @Override
    public List<MetricsTag> tags() {
      return tags;
    }

    @Override
    public Iterable<AbstractMetric> metrics() {
      return metrics;
    }
  }

  public static void main(String[] args) throws Exception {
    ConfigBuilder cb = new ConfigBuilder();
    if (args.length > 0) {
      cb.add("test.sink.influxdb.servers", args[0]);
    }
    if (args.length > 1) {
      cb.add("test.sink.influxdb.protocol", args[1]);
    }
    if (args.length > 2) {
      cb.add("test.sink.influxdb.db", args[2]);
    }
    InfluxDBSink sink = new InfluxDBSink();
    sink.init(cb.subset("test.sink.influxdb"));
    sink.putMetrics(getTestRecord(System.currentTimeMillis(), 10));
    Thread.sleep(1000);
    sink.putMetrics(getTestRecord(System.currentTimeMillis(), 20));
    sink.flush();
  }
}
