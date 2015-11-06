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

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.MetricsVisitor;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.sink.InfluxDBSink;
import org.junit.Test;
import org.junit.Assert;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestInfluxDBSink {
  @Test
  public void testBuildLine() {
    MetricsRecord record = getTestRecord();
    Assert.assertEquals(
        "test.name1,Context=test,Hostname=host1,ProcessName=process\\ name" +
        " metric1=1.0,metric2=2 10000000\n",
        InfluxDBSink.buildLine(new StringBuilder(), record).toString());
  }

  @Test
  public void testINfluxDBSink() {
    InfluxDBSink sink = new InfluxDBSink();
    ConfigBuilder cb = new ConfigBuilder()
        .add("test.sink.influxdb.servers", "localhost:8086")
        .add("test.sink.influxdb.db", "mydb");
    sink.init(cb.subset("test.sink.influxdb"));
  }

  private MetricsRecord getTestRecord() {
    List<MetricsTag> tags = new ArrayList<MetricsTag>();
    tags.add(new MetricsTag(MsInfo.Context, "test"));
    tags.add(new MetricsTag(MsInfo.Hostname, "host1"));
    tags.add(new MetricsTag(MsInfo.ProcessName, "process name"));
    List<AbstractMetric> metrics = new ArrayList<AbstractMetric>();
    metrics.add(new TestMetric("metric1", 1.0));
    metrics.add(new TestMetric("metric2", 2));
    MetricsInfo info = new TestMetricsInfo("name1");
    return new TestMetricsRecord(info, (long) 10000, tags, metrics);
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

  private class TestMetricsRecord extends AbstractMetricsRecord {
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
}
