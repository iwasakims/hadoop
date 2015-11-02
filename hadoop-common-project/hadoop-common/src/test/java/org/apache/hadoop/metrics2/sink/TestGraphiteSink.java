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

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.sink.GraphiteSink;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import static org.junit.Assert.assertEquals;

public class TestGraphiteSink {
  @Test
  public void testDefaultConfig() {
    GraphiteSink sink = new GraphiteSink();
    try {
      SubsetConfiguration subset =
          new SubsetConfiguration(new PropertiesConfiguration(), "dummy");
      sink.init(subset);
    } catch (MetricsException e) {
      // ignore connection failure.
    }
    GraphiteSink.Graphite graphite =
        (GraphiteSink.Graphite)Whitebox.getInternalState(sink, "graphite");
    Assert.assertEquals(GraphiteSink.SERVER_HOST_DEFAULT,
        (String)Whitebox.getInternalState(graphite, "serverHost"));
    Assert.assertEquals(GraphiteSink.SERVER_PORT_DEFAULT,
        (Whitebox.getInternalState(graphite, "serverPort")).toString());
  }

  @Test
  public void testServerConfig() {
    GraphiteSink sink = new GraphiteSink();
    String serverHost = "127.0.0.1";
    String serverPort = "2002";
    try {
      PropertiesConfiguration conf = new PropertiesConfiguration();
      SubsetConfiguration subset = new SubsetConfiguration(conf, "dummy");
      subset.addPropertyDirect(GraphiteSink.SERVER_HOST_KEY, serverHost);
      subset.addPropertyDirect(GraphiteSink.SERVER_PORT_KEY, serverPort);
      sink.init(subset);
    } catch (MetricsException e) {
      // ignore connection failure.
    }
    GraphiteSink.Graphite graphite =
        (GraphiteSink.Graphite)Whitebox.getInternalState(sink, "graphite");
    Assert.assertEquals(serverHost,
        (String)Whitebox.getInternalState(graphite, "serverHost"));
    Assert.assertEquals(serverPort,
        (Whitebox.getInternalState(graphite, "serverPort")).toString());
  }
}
