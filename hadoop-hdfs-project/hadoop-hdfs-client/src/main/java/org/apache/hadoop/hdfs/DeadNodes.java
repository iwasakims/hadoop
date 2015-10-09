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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * DeadNodes contains datanodes from which client failed to read data and
 * assumed to be dead. The entry is evicted after certain period because
 * the node is possible to be restarted and make client try again.
 */
@InterfaceAudience.Private
public class DeadNodes {
  private final LoadingCache<DatanodeInfo, DatanodeInfo> deadNodes;

  DeadNodes(long deadNodesCacheExpiry) {
    deadNodes = CacheBuilder.newBuilder()
        .expireAfterWrite(deadNodesCacheExpiry, TimeUnit.MILLISECONDS)
        .build(new CacheLoader<DatanodeInfo, DatanodeInfo>() {
            @Override
            public DatanodeInfo load(DatanodeInfo key) throws Exception {
              return key;
            }
          });
  }

  Set<DatanodeInfo> getNodes() {
    return deadNodes.getAllPresent(deadNodes.asMap().keySet()).keySet();
  }

  void put(DatanodeInfo dnInfo) {
    deadNodes.put(dnInfo, dnInfo);
  }

  void clear() {
    deadNodes.invalidateAll();
  }

  boolean contains(DatanodeInfo dnInfo) {
    return (deadNodes.getIfPresent(dnInfo) != null);
  }

  void remove(DatanodeInfo dnInfo) {
    deadNodes.invalidate(dnInfo);
  }
}
