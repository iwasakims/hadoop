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

package org.apache.hadoop.fs.shell;

import java.io.IOException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.tracing.SpanReceiverHost;
import org.apache.hadoop.util.ToolRunner;
import org.htrace.Sampler;
import org.htrace.TraceScope;

/**
 * Get a listing of all files in that match the file patterns.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable

class Trace extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Trace.class, "-trace");
  }
  
  public static final String NAME = "trace";
  public static final String USAGE = "<command> [<args> ...]";
  public static final String DESCRIPTION =
      "invoke fs shell command with tracing turned on.";
  SpanReceiverHost spanReceiverHost;

  @Override
  public int run(String...argv) {
    FsShell shell = new FsShell();
    shell.setConf(getConf());
    SpanReceiverHost spanReceiverHost = SpanReceiverHost.getInstance(getConf());
    TraceScope ts = org.htrace.Trace.startSpan(shell.getClass().getSimpleName(),
        Sampler.ALWAYS);
    int res = -1;
    try {
      res = ToolRunner.run(shell, argv);
    } catch (Exception e) {
      displayError(e);
    } finally {
      ts.close();
      spanReceiverHost.closeReceivers();
    }
    try {
      shell.close();
    } catch (IOException e) {
      displayError(e);
    }
    return res;
  }
}
