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
import org.apache.hadoop.net.NetUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import java.io.IOException;
import java.nio.charset.Charset;
import java.net.InetSocketAddress;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.List;

/**
 * A metrics sink that writes to a InfluxDB
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class InfluxDBSink implements MetricsSink {
  private static final Log LOG = LogFactory.getLog(InfluxDBSink.class);
  private static final Charset UTF8 = Charset.forName("UTF-8");
  static final String PROTOCOL_KEY = "protocol";
  static final String PROTOCOL_DEFAULT = "http";
  static final String SERVERS_KEY = "servers";
  static final int PORT_DEFAULT = 8086;
  static final String DB_KEY = "db";
  static final String DB_DEFAULT = "mydb";
  private final StringBuilder builder = new StringBuilder();
  private InfluxDB influxdb;

  @Override
  public void init(SubsetConfiguration conf) {
    influxdb = getInfluxDB(conf);
  }

  @Override
  public void putMetrics(MetricsRecord record) {
    builder.setLength(0);
    influxdb.putLine(buildLine(builder, record).toString());
  }
  
  @Override
  public void flush() {
  }

  /**
   * Create metrics record expression in Line Protocol syntax of InfluxDB.
   */
  static StringBuilder buildLine(StringBuilder buf, MetricsRecord rec) {
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
    buf.append(rec.timestamp() * 1000000L);

    buf.append("\n");

    return buf;
  }

  static InfluxDB getInfluxDB(SubsetConfiguration conf) {
    String protocol =
        conf.getString(PROTOCOL_KEY, PROTOCOL_DEFAULT).toUpperCase();
    InfluxDB influxdb =
        protocol.equals("HTTP")? new HttpInfluxDB() : new UdpInfluxDB();
    influxdb.init(conf);
    return influxdb;
  }

  interface InfluxDB {
    void init(SubsetConfiguration conf);
    void putLine(String record);
  }

  static class HttpInfluxDB implements InfluxDB {
    private HttpClient client;
    private HttpPost post;

    @Override
    public void init(SubsetConfiguration conf) {
      client = new DefaultHttpClient();
      List<InetSocketAddress> servers =
          Servers.parse(conf.getString(SERVERS_KEY), PORT_DEFAULT);
      String url =
          "http://" + NetUtils.getHostPortString(servers.get(0)) +
          "/write?db=" + conf.getString(DB_KEY, DB_DEFAULT);
      post = new HttpPost(url);
    }

    @Override
    public void putLine(String record) {
      try {
        post.setEntity(new ByteArrayEntity(record.getBytes(UTF8)));
        HttpResponse response = client.execute(post);
      } catch (IOException e) {
        LOG.debug("Error while posting metrics record.", e);
      }
    }

    String getURI() {
      return post.getURI().toString();
    }
  }

  static class UdpInfluxDB implements InfluxDB {
    private DatagramSocket socket;
    private List<InetSocketAddress> servers;
    
    @Override
    public void init(SubsetConfiguration conf) {
      try {
        socket = new DatagramSocket();
      } catch (IOException e) {
        LOG.error(e);
      }
      servers = Servers.parse(conf.getString(SERVERS_KEY), PORT_DEFAULT);
    }

    @Override
    public void putLine(String record) {
      if (socket != null) {
        try {
          for (InetSocketAddress addr : servers) {
            byte[] buf = record.getBytes(UTF8);
            socket.send(new DatagramPacket(buf, buf.length, addr));
          }
        } catch (IOException e) {
          LOG.debug(e);
          throw new MetricsException("Failed to putMetrics", e);
        }
      }
    }
  }
}
