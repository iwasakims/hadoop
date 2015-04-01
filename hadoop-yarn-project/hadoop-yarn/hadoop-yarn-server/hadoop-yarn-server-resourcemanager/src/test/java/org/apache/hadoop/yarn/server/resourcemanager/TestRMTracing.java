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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.tracing.TraceAdmin;
import org.apache.hadoop.tracing.TestTracing;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.junit.Assert;
import org.junit.Test;

public class TestRMTracing {

  private int runTraceCommand(TraceAdmin trace, String... cmd)
      throws Exception {
    return trace.run(cmd);
  }

  @Test
  public void testRMTracing() throws Exception {
    Configuration conf = new YarnConfiguration();
    conf.set(SpanReceiverHost.SPAN_RECEIVERS_CONF_KEY,
        TestTracing.SetSpanReceiver.class.getName());
    MockRMForTestTracing rm = new MockRMForTestTracing(conf);
    try {
      rm.start();
      String hostPort = "127.0.0.1:" +
          rm.getRMContext().getRMAdminService().getServer().getPort();
      TraceAdmin traceAdmin = new TraceAdmin();
      traceAdmin.setConf(conf);
      TraceScope ts = Trace.startSpan("testRMTracing", Sampler.ALWAYS);
      Assert.assertEquals(0, 
          runTraceCommand(traceAdmin, "-list", "-host", hostPort));
      ts.close();
      String[] expectedSpanNames = {
        "testRMTracing",
        "TraceAdminService#listSpanReceivers",
        "org.apache.hadoop.tracing.TraceAdminPB.TraceAdminService.listSpanReceivers"
      };
      TestTracing.assertSpanNamesFound(expectedSpanNames);
    } finally {
      rm.stop();
    }
  }

  private class MockRMForTestTracing extends MockRM {
    public MockRMForTestTracing(Configuration conf) {
      super(conf);
    }

    @Override
    protected AdminService createAdminService() {
      return new AdminService(this, rmContext);
    }
  }
}
