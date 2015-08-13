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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
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
        1, 1, 1);
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
  public void testNMTracing() throws Exception {
    Configuration conf = cluster.getConfig();
    NMTokenCache nmTokenCache = new NMTokenCache();

    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    rmClient.setNMTokenCache(nmTokenCache);
    rmClient.init(conf);
    rmClient.start();

    NMClientImpl nmClient = (NMClientImpl) NMClient.createNMClient();
    nmClient.setNMTokenCache(rmClient.getNMTokenCache());
    nmClient.init(conf);
    nmClient.start();

    ContainerId containerId =
        ContainerId.fromString("container_1427562100000_0001_01_000001");
    NodeId nodeId = cluster.getNodeManager(0).getNMContext().getNodeId();
    nmClient.getContainerStatus(containerId, nodeId);
  }

  @Test
  public void testRMTracing() throws Exception {
    YarnClient client = YarnClient.createYarnClient();
    client.init(cluster.getConfig());
    client.start();

    try (TraceScope ts = Trace.startSpan("testRMTracing", Sampler.ALWAYS)) {
      client.getApplications();
    }

    client.stop();
    String[] expectedSpanNames = {
      "testRMTracing",
      "ApplicationClientProtocolPB#getApplications",
      "ApplicationClientProtocolService#getApplications"
    };
    SetSpanReceiver.assertSpanNamesFound(expectedSpanNames);
  }

  @Test
  public void testRMTraceAdmin() throws Exception {
    Configuration conf = cluster.getConfig();
    String hostPort = conf.get(YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS);
    TraceAdmin traceAdmin = new TraceAdmin();
    traceAdmin.setConf(conf);

    Assert.assertEquals(0,
        runTraceCommand(traceAdmin, "-list", "-host", hostPort));
    Assert.assertEquals(0,
        runTraceCommand(traceAdmin, "-remove", "1","-host", hostPort));
    Assert.assertEquals(0,
        runTraceCommand(traceAdmin,
            "-add", "-class", SetSpanReceiver.class.getName(),
            "-host", hostPort));

    try (TraceScope ts = Trace.startSpan("traceAdmin", Sampler.ALWAYS)) {
      runTraceCommand(traceAdmin, "-list", "-host", hostPort);
    }

    String[] expectedSpanNames = {
      "traceAdmin",
      "TraceAdminService#listSpanReceivers",
    };
    SetSpanReceiver.assertSpanNamesFound(expectedSpanNames);
  }

  private static int runTraceCommand(TraceAdmin trace, String... cmd)
      throws Exception {
    return trace.run(cmd);
  }
}
