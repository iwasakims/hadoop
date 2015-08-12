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

package org.apache.hadoop.yarn.client;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.SetSpanReceiver;
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.tracing.TraceAdmin;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestYARNTracing {
  private static MiniYARNCluster cluster;

  @BeforeClass
  public static void setup() throws IOException {
    Configuration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.YARN_SERVER_HTRACE_PREFIX +
        SpanReceiverHost.SPAN_RECEIVERS_CONF_SUFFIX,
        SetSpanReceiver.class.getName());
    cluster = new MiniYARNCluster(TestYARNTracing.class.getSimpleName(),
        0, 1, 1);
    cluster.init(conf);
    cluster.start();
  }

  @AfterClass
  public static void teardown() {
    if (cluster != null) {
      cluster.stop();
      cluster = null;
    }
  }

  @Before
  public void clearSpans() {
    SetSpanReceiver.clear();
  }

  @Test
  public void testRMTracing() throws Exception {
    YarnClient client = YarnClient.createYarnClient();
    client.init(cluster.getConfig());
    client.start();

    try (TraceScope ts = Trace.startSpan("testRMTracing", Sampler.ALWAYS)) {
      Thread.sleep(5000);
      client.getApplications();
      String[] expectedSpanNames = {
        "testRMTracing",
      };
      SetSpanReceiver.assertSpanNamesFound(expectedSpanNames);
      for (String description : SetSpanReceiver.getMap().keySet()) {
        System.out.println("##### " + description);
      }
    } finally {
      client.stop();
    }
  }

  private static int runTraceCommand(TraceAdmin trace, String... cmd)
      throws Exception {
    return trace.run(cmd);
  }
}
