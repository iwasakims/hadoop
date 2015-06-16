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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
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
  @Test
  public void testDeadNodes() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    try {
    DistributedFileSystem fs = cluster.getFileSystem();
    DFSClient client = fs.getClient();
    String filename = "/testfile";
    Path filepath = new Path(filename);
    DFSTestUtil.createFile(fs, filepath, 1024, (short) 3, 0);
    DFSInputStream in = client.open(filename);
    assertTrue(in.seekToNewSource(1));
    DatanodeInfo dn = in.getCurrentDatanode();
    in.addToDeadNodes(dn);
    assertTrue(client.getClientContext().getDeadNodes().containsKey(dn));
    } finally {
      cluster.shutdown();
    }
  }
}
