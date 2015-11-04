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

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import java.io.IOException;

/**
 * A metrics sink that writes to a Graphite server
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InfluxDBSink implements MetricsSink {
  private static final Log LOG = LogFactory.getLog(InfluxDBSink.class);
  public static final String ADDRESS_KEY = "address";
  public static final String ADDRESS_DEFAULT = "address";
  private InfluxDB influxdb = new InfluxDB();
  private final StringBuilder builder = new StringBuilder();

  @Override
  public void init(SubsetConfiguration conf) {
    influxdb.init(conf);
  }

  @Override
  public void putMetrics(MetricsRecord record) {
    builder.setLength(0);
    influxdb.putLine(buildLine(builder, record).toString());
  }
  
  @Override
  public void flush() {
    influxdb.flush();
  }

  public static StringBuilder buildLine(StringBuilder buf, MetricsRecord rec) {
    // measurement
    buf.append(rec.context())
       .append(".")
       .append(rec.name());

    // tags
    for (MetricsTag tag : rec.tags()) {
      if (tag.value() != null) {
        buf.append(",")
           .append(tag.name())
           .append("=")
           .append(tag.value().replace(" ", "\\ "));
      }
    }

    buf.append(" ");

    // fields
    String prefix = "";
    for (AbstractMetric metric : rec.metrics()) {
      buf.append(prefix)
         .append(metric.name().replace(" ", "\\ "))
         .append("=")
         .append(metric.value());
      prefix = ",";
    }

    buf.append(" ");
    
    // The record timestamp is in milliseconds
    // while InfluxDB expects nanoseconds.
    buf.append(rec.timestamp() * 1000L);

    buf.append("\n");

    return buf;
  }
  
  public static class InfluxDB {
    public void init(SubsetConfiguration conf) {
    }
    
    public void putLine(String record) {
    }

    public void flush() {
    }
  }
}
