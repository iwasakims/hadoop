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

package org.apache.hadoop.fs.azurebfs;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.ClientDistributedCacheManager;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

/**
 * Test distributed cache with ABFS.
 */
public class ITestAbfsDistributedCache extends AbstractAbfsIntegrationTest {
  private static final Path TEST_FILE_PATH = new Path("/testfile");
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAbfsDistributedCache.class);

  public ITestAbfsDistributedCache() throws Exception {
    super();
  }

  @Test
  public void testCacheFileVisibility() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    FSDataOutputStream out = fs.create(TEST_FILE_PATH);
    out.close();
    fs.setPermission(TEST_FILE_PATH, new FsPermission("755"));
    Job job = Job.getInstance(fs.getConf());
    job.addCacheFile(TEST_FILE_PATH.toUri());
    Configuration conf = job.getConfiguration();
    Map<URI, FileStatus> statCache = new HashMap<>();
    ClientDistributedCacheManager.determineCacheVisibilities(conf, statCache);
    Assert.assertArrayEquals("The file should not be visible",
        new boolean[]{false}, DistributedCache.getFileVisibilities(conf));
  }
}
