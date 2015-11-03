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
import org.apache.hadoop.metrics2.util.Servers;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;

/**
 * A metrics sink that writes to a Graphite server
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InfluxDBSink implements MetricsSink {
  private static final Log LOG = LogFactory.getLog(InfluxDBSink.class);
  public static final String SERVERS_PROPERTY = "servers";
  public static final int DEFAULT_PORT = 8089;
  private List<? extends SocketAddress> metricsServers;
  private final StringBuilder buffer = new StringBuilder();

  @Override
  public void init(SubsetConfiguration conf) {
    metricsServers =
        Servers.parse(conf.getString(SERVERS_PROPERTY), DEFAULT_PORT);
  }

  @Override
  public void putMetrics(MetricsRecord record) {
    buffer.setLength(0);

    // measurement
    buffer.append(record.context())
          .append(".")
          .append(record.name());

    // tags
    for (MetricsTag tag : record.tags()) {
      if (tag.value() != null) {
        buffer.append(",")
              .append(tag.name())
              .append("=")
              .append(tag.value().replace(" ", "\\ "));
      }
    }

    buffer.append(" ");

    // fields
    String prefix = "";
    for (AbstractMetric metric : record.metrics()) {
      buffer.append(prefix)
            .append(metric.name().replace(" ", "\\ "))
            .append("=")
        .append(metric.value());
      prefix = ",";
    }

    buffer.append(" ");
    
    // The record timestamp is in milliseconds
    // while InfluxDB expects an nanoseconds.
    buffer.append(record.timestamp() * 1000L);

    buffer.append("\n");
  }

  @Override
  public void flush() {
  }
}
