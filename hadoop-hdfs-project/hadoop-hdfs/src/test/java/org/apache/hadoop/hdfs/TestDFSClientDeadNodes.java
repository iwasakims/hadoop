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
package org.apache.hadoop.hdfs;

import com.google.common.base.Supplier;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream.DeadNodes;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

/**
 * These tests make sure that DFSClient shares deadNodes list and 
 * the entries evicted by configured timeout.
 */
public class TestDFSClientDeadNodes {
  @Test(timeout=60000)
  public void testDeadNodes() throws IOException {
    Configuration conf = new Configuration();
    int expiry = 500; // short expiry period for test
    conf.setLong(HdfsClientConfigKeys.Read.DEAD_NODES_CACHE_EXPIRY_INTERVAL_KEY,
        Long.valueOf(expiry));
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
      DistributedFileSystem fs = cluster.getFileSystem();
      DFSClient client = fs.getClient();
      String filename = "/testfile";
      DFSTestUtil.createFile(fs, new Path(filename), 1024, (short) 3, 0);
      DFSInputStream in = client.open(filename);
      in.seekToNewSource(1);
      final DatanodeInfo dn = in.getCurrentDatanode();
      in.addToDeadNodes(dn);

      // check if shared deadNodes contains added datanode.
      final DeadNodes deadNodes = client.getClientContext().getDeadNodes();
      assertTrue(deadNodes.contains(dn));

      // making sure that the entry in deadNodes is expired.
      try {
        GenericTestUtils.waitFor(new Supplier<Boolean>() {
          @Override
          public Boolean get() {
            return !deadNodes.contains(dn);
          }
        }, 100, expiry * 2);
      } catch (TimeoutException e) {
        fail("timed out while waiting cache expiry: " + e.getMessage());
      } catch (InterruptedException e) {
        fail("interrupted while waiting cache expiry: " + e.getMessage());
      }
    } finally {
      cluster.shutdown();
    }
  }
}
