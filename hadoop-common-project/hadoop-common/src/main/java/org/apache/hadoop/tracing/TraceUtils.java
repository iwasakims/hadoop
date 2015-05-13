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
package org.apache.hadoop.tracing;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair;
import org.apache.htrace.HTraceConfiguration;

/**
 * This class provides utility functions for tracing.
 */
@InterfaceAudience.Private
public class TraceUtils {
  private static List<ConfigurationPair> EMPTY = Collections.emptyList();

  public static HTraceConfiguration wrapHadoopConf(final String prefix,
        final Configuration conf) {
    return wrapHadoopConf(prefix, conf, EMPTY);
  }

  public static HTraceConfiguration wrapHadoopConf(final String prefix,
        final Configuration conf, List<ConfigurationPair> extraConfig) {
    final HashMap<String, String> extraMap = new HashMap<String, String>();
    for (ConfigurationPair pair : extraConfig) {
      extraMap.put(pair.getKey(), pair.getValue());
    }
    return new HTraceConfiguration() {
      @Override
      public String get(String key) {
        return get(key, "");
      }

      @Override
      public String get(String key, String defaultValue) {
        String prefixedKey = prefix + key;
        if (extraMap.containsKey(prefixedKey)) {
          return extraMap.get(prefixedKey);
        } else if (extraMap.containsKey(key)) {
          // allow users to omit prefix for extra configuration properties to
          // reduce typing. The prefix here is obvious from the target of
          // "hadoop trace" command.
          return extraMap.get(key);
        } else {
          return conf.get(prefixedKey, defaultValue);
        }
      }
    };
  }
}
