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

package org.apache.hadoop;

import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

/**
 * Verify mini cluster works with declared dependencies.
 * Because dependencies of test-jar is not transitive by design (MNG-1378),
 * mini cluster throws NoClassDefFoundError if it depends on artifacts
 * depended by only test-jars.
 */
public class TestMiniCluster {

  @Test(timeout=60000)
  public void testMiniDFSCluster() throws Throwable {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    cluster.shutdown();
  }

  @Test(timeout=60000)
  public void testMiniYARNCluster() throws Throwable {
    Configuration conf = new YarnConfiguration();
    MiniYARNCluster cluster =
        new MiniYARNCluster("testMiniYARNCluster", 1, 3, 1, 1);
    cluster.init(conf);
    cluster.start();
    cluster.waitForNodeManagersToConnect(10000);
    cluster.stop();
  }
}
