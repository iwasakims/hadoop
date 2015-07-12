/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.fs.contract.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractSnapshotTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHDFSContractSnapshot extends AbstractContractSnapshotTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHDFSContractSnapshot.class);

  @BeforeClass
  public static void createCluster() throws IOException {
    HDFSContract.createCluster();
  }

  @AfterClass
  public static void teardownCluster() throws IOException {
    HDFSContract.destroyCluster();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new HDFSContract(conf);
  }

  @Override 
  public void setup() throws Exception {
    super.setup();
    MiniDFSCluster cluster = HDFSContract.getCluster();
    Configuration conf = cluster.getConfiguration(0);
    Path path = getSnapshotPath();
    ToolRunner.run(new DFSAdmin(conf),
        new String[]{"-allowSnapshot", path.toString()});
  }

  @Test
  public void testReservedPathElement() throws Throwable {
    try {
      getFileSystem().create(new Path(HdfsConstants.DOT_SNAPSHOT_DIR));
      fail("Expected a failure");
    } catch (IOException e) {
      LOG.info(e.getMessage());
    }
  }
}
