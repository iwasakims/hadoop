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

  private Path targetPath;
  private final String snapshotName1 = "snapshotName1";
  private final String snapshotName2 = "snapshotName2";

  @Override
  public void setup() throws Exception {
    super.setup();
    skipIfUnsupported(SUPPORTS_SNAPSHOT);
    targetPath = path("testSnapshot");
    getFileSystem().mkdirs(targetPath);
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
  public void testCreateAndRenameSnapshot() throws Exception {
    FileSystem fs = getFileSystem();
    Path snapshotPath = fs.createSnapshot(targetPath, snapshotName1);
    assertTrue(fs.exists(snapshotPath));
    fs.renameSnapshot(targetPath, snapshotName1, snapshotName2);
    assertFalse(fs.exists(snapshotPath));
  }

  @Test
  public void testCreateAndDeleteSnapshot() throws Exception {
    FileSystem fs = getFileSystem();
    Path snapshotPath = fs.createSnapshot(targetPath, snapshotName1);
    assertTrue(fs.exists(snapshotPath));
    fs.deleteSnapshot(targetPath, snapshotName1);
    assertFalse(fs.exists(snapshotPath));
  }

  public Path getTargetPath() {
    return targetPath;
  }

}
