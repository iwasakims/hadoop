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
package org.apache.hadoop.fs;

import junit.framework.AssertionFailedError;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.SetSpanReceiver;
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.util.ToolRunner;
import org.apache.htrace.SamplerBuilder;
import org.apache.htrace.impl.AlwaysSampler;
import org.junit.Test;

public class TestFsShell {

  @Test
  public void testConfWithInvalidFile() throws Throwable {
    String[] args = new String[1];
    args[0] = "--conf=invalidFile";
    Throwable th = null;
    try {
      FsShell.main(args);
    } catch (Exception e) {
      th = e;
    }

    if (!(th instanceof RuntimeException)) {
      throw new AssertionFailedError("Expected Runtime exception, got: " + th)
          .initCause(th);
    }
  }

  @Test
  public void testTracing() throws Throwable {
    Configuration conf = new Configuration();
    String prefix = FsShell.SEHLL_HTRACE_PREFIX;
    conf.set(prefix + SpanReceiverHost.SPAN_RECEIVERS_CONF_SUFFIX,
        SetSpanReceiver.class.getName());
    conf.set(prefix + SamplerBuilder.SAMPLER_CONF_KEY,
        AlwaysSampler.class.getName());
    conf.setQuietMode(false);
    FsShell shell = new FsShell(conf);
    int res;
    try {
      res = ToolRunner.run(shell, new String[]{"-help"});
    } finally {
      shell.close();
    }
    SetSpanReceiver.assertSpanNamesFound(new String[]{"help"});
  }
}
