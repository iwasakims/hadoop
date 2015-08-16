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

import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.tracing.SetSpanReceiver;
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.tracing.TraceAdmin;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.apache.htrace.Sampler;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestYARNTracing {
  static final Log LOG = LogFactory.getLog(TestYARNTracing.class);
  private static MiniYARNCluster cluster;
  private static YarnClient yarnClient;

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
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(cluster.getConfig());
    yarnClient.start();
  }

  @AfterClass
  public static void teardown() {
    if (yarnClient != null) {
      yarnClient.stop();
      yarnClient = null;
    }
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
    ApplicationSubmissionContext appContext = 
        yarnClient.createApplication().getApplicationSubmissionContext();
    appContext.setApplicationName("TestNMTracing");
    appContext.setPriority(Priority.newInstance(0));
    appContext.setQueue("default");
    appContext.setAMContainerSpec(
        Records.newRecord(ContainerLaunchContext.class));
    appContext.setUnmanagedAM(true);
    yarnClient.submitApplication(appContext);

    // wait for app to start
    final YarnClient yarnClient = TestYARNTracing.yarnClient;
    final ApplicationId appId = appContext.getApplicationId();
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        try {
          ApplicationReport report = yarnClient.getApplicationReport(appId);
          if (report.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
            RMAppAttempt appAttempt =
                cluster.getResourceManager()
                  .getRMContext().getRMApps().get(appId).getCurrentAppAttempt();
            if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
              return true;
            }
          }
        } catch (Exception e) {
          LOG.info(StringUtils.stringifyException(e));
          Assert.fail("Exception while getting application state.");
        }
        return false;
      }
    }, 1000, 30000);

    List<NodeReport> nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    String node = nodeReports.get(0).getNodeId().getHost();
    String rack = nodeReports.get(0).getRackName();
    String[] nodes = new String[] {node};
    String[] racks = new String[] {rack};
    Resource capability = Resource.newInstance(1024, 0);
    Priority priority = Priority.newInstance(0);

    RMAppAttempt appAttempt =
        cluster.getResourceManager()
            .getRMContext().getRMApps().get(appId).getCurrentAppAttempt();
    UserGroupInformation.setLoginUser(
        UserGroupInformation.createRemoteUser(
            UserGroupInformation.getCurrentUser().getUserName()));
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());

    NMTokenCache nmTokenCache = new NMTokenCache();
    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    rmClient.setNMTokenCache(nmTokenCache);
    rmClient.init(cluster.getConfig());
    rmClient.start();
    Container container = null;
    try {
      rmClient.registerApplicationMaster("TestNMTracing", 10000, "");
      rmClient.addContainerRequest(
          new ContainerRequest(capability, nodes, racks, priority));
      for (int i = 5; i > 0; i--) {
        AllocateResponse allocResponse = rmClient.allocate(0.1f);
        if (allocResponse.getAllocatedContainers().size() > 0) {
          container = allocResponse.getAllocatedContainers().get(0);
          break;
        }
        Thread.sleep(1000);
      }
    } finally {
      rmClient.stop();
    }
    if (container == null) {
      Assert.fail("Failed to allocate containers.");
    }

    NMClient nmClient = NMClient.createNMClient();
    nmClient.setNMTokenCache(nmTokenCache);
    nmClient.init(cluster.getConfig());
    nmClient.start();
    try (TraceScope ts = Trace.startSpan("testNMTracing", Sampler.ALWAYS)) {
      nmClient.getContainerStatus(container.getId(), container.getNodeId());
      // getContainerStatus called before startContainer is expected
      // to throw exception. It is not problem here because this test
      // just checks whether server side tracing span is get or not.
      Assert.fail("Exception is expected");
    } catch (YarnException e) {
      LOG.info(StringUtils.stringifyException(e));
    } finally {
      nmClient.close();
    }
    String[] expectedSpanNames = {
      "testNMTracing",
      "ContainerManagementProtocolPB#getContainerStatuses",
      "ContainerManagementProtocolService#getContainerStatuses"
    };
    SetSpanReceiver.assertSpanNamesFound(expectedSpanNames);
  }

  @Test
  public void testRMTracing() throws Exception {
    try (TraceScope ts = Trace.startSpan("testRMTracing", Sampler.ALWAYS)) {
      yarnClient.getApplications();
    }
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
