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

import static org.junit.Assert.fail;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;


/**
 * These tests make sure that DFSClient retries fetching data from DFS
 * properly in case of errors. Including deadNodes list manager function.
 */
public class TestDFSClientDeadNodes {
  Configuration conf = new Configuration();
  MiniDFSCluster cluster;
  final KillDatanodeAsync killDatanodeAsync = new KillDatanodeAsync();

  @Test
  public void testDeadNodes() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    DistributedFileSystem fs = cluster.getFileSystem();
    Path filePath = new Path("/testDeadNodes");
    DFSTestUtil.createFile(fs, new Path("/testfile"), 1024, (short) 3, 0);

    
    DFSClient client = fs.dfs;
    OutputStream out = fs.create(filePath, true, 4096);
    out.write(4096);

    try {
      out.close();
    } catch (Exception e) {
      fail("DataNode failure should not result in a block abort: \n" + e.getMessage());
    }

    this.killDatanodeAsync.start();

    for (int x = 0; x<7000; x++)  {
      InputStream in = fs.open(filePath);
      byte[] b = new byte[4096];
      in.read(b,0,2048);
      try {
        in.close();
      } catch (Exception e) {
        fail("DataNode failure should not result in a block abort: \n" + e.getMessage());
      }
    }

    /* sleep long enough for datanode to come back, then test deadNode to be empty */
    try {
      Thread.sleep(5000);
      if(client.getDeadNodeCount() > 0)
        fail ("Datanode deadlist should be empty " + client.getDeadNodeCount());

    } catch (InterruptedException e) {}
  }

  private class KillDatanodeAsync extends Thread {
    public void run() {
      try {
        /* wait for reading to start */
        Thread.sleep(1000);
        try {
          /* simulate absence of datanode */
          cluster.restartDataNode(0, true);
        } catch (IOException e) {
        }
      } catch (InterruptedException e) {
      }
    }
  }
}
