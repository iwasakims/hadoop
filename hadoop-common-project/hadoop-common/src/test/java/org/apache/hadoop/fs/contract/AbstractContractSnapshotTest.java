/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Snapshot operations
 */
public abstract class AbstractContractSnapshotTest extends AbstractFSContractTestBase {

  private Path snapshotPath;
  private final String snapshotName1 = "snapshotName1";
  private final String snapshotName2 = "snapshotName2";

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_SNAPSHOT);
    snapshotPath = path("testSnapshot");
    getFileSystem().mkdirs(snapshotPath);
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    return conf;
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
  }

  @Test
  public void testSnapshot() throws Exception {
    FileSystem fs = getFileSystem();
    fs.createSnapshot(snapshotPath, snapshotName1);
  }

  public Path getSnapshotPath() {
    return snapshotPath;
  }

}
