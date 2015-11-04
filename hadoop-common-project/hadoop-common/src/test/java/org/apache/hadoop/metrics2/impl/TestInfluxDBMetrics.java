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

package org.apache.hadoop.metrics2.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.sink.InfluxDBSink;
import org.junit.Test;
import org.junit.Assert;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestInfluxDBMetrics {
  private static final Log LOG = LogFactory.getLog(TestInfluxDBMetrics.class);

  @Test
  public void testGetLine() {
    List<MetricsTag> tags = new ArrayList<MetricsTag>();
    tags.add(new MetricsTag(MsInfo.Context, "test"));
    tags.add(new MetricsTag(MsInfo.Hostname, "host1"));
    tags.add(new MetricsTag(MsInfo.ProcessName, "process name"));
    List<AbstractMetric> metrics = new ArrayList<AbstractMetric>();
    metrics.add(makeMetric("metric1", 1.0));
    metrics.add(makeMetric("metric2", 2));
    MetricsInfo info = new MetricsInfo() {
        public String name() { return "name1"; }
        public String description() { return "metrics description."; }
      };
    MetricsRecord record =
        new MetricsRecordImpl(info, (long) 10000, tags, metrics);

    StringBuilder builder = new StringBuilder();
    InfluxDBSink.buildLine(builder, record);

    Assert.assertEquals(
        "test.name1,Context=test,Hostname=host1,ProcessName=process\\ name" +
        " metric1=1.0,metric2=2 10000000\n",
        builder.toString());
  }

  @Test
  public void testINfluxDBSink() {
    InfluxDBSink sink = new InfluxDBSink();
    ConfigBuilder cb = new ConfigBuilder()
        .add("test.sink.influxdb.address", "localhost")
        .add("test.sink.influxdb.dummy", "true");
    sink.init(cb.subset("test.sink.influxdb"));
  }

  private AbstractMetric makeMetric(String name, Number value) {
    AbstractMetric metric = mock(AbstractMetric.class);
    when(metric.name()).thenReturn(name);
    when(metric.value()).thenReturn(value);
    return metric;
  }
}
