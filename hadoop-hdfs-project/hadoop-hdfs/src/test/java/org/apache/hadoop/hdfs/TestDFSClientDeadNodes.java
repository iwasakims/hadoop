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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
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
    DFSClient client = fs.getClient();
    String testfile = "/testfile";
    DFSTestUtil.createFile(fs, new Path(testfile), 1024, (short) 3, 0);
    DFSInputStream in = client.open(testfile);
    in.seekToNewSource(0);
    DatanodeInfo dn = in.getCurrentDatanode();
    System.out.println("##### " + dn);
    in.addToDeadNodes(dn);
    in.read();

    List<LocatedBlock> blocks = in.getAllBlocks();
    for (LocatedBlock block : blocks) {
      System.out.println("##### " + block);
    }

    /*
    this.killDatanodeAsync.start();

    for (int x = 0; x<7000; x++)  {
      InputStream in = fs.open(filePath);
      byte[] b = new byte[1024];
      in.read(b, 0, 1024);
      try {
        in.close();
      } catch (Exception e) {
        fail("DataNode failure should not result in a block abort: \n" + e.getMessage());
      }
    }

    try {
      Thread.sleep(5000);
      if(client.getDeadNodeCount() > 0)
        fail ("Datanode deadlist should be empty " + client.getDeadNodeCount());
    } catch (InterruptedException e) {
    }
    */
  }

  private class KillDatanodeAsync extends Thread {
    public void run() {
      try {
        /* wait for reading to start */
        Thread.sleep(1000);
        try {
          cluster.restartDataNode(0, true);  // simulate absence of datanode
        } catch (IOException e) {
        }
      } catch (InterruptedException e) {
      }
    }
  }
}
