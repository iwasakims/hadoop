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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Sampler;
import org.apache.htrace.SamplerBuilder;
import org.apache.htrace.impl.ProbabilitySampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class TraceSamplerFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(TraceSamplerFactory.class);
  private static final String HTRACE_CONF_PREFIX ="hadoop.htrace.";

  public static Sampler<?> createSampler(Configuration conf) {
    String samplerStr = conf.get(CommonConfigurationKeys.HADOOP_TRACE_SAMPLER,
        CommonConfigurationKeys.HADOOP_TRACE_SAMPLER_DEFAULT);
    conf.set(HTRACE_CONF_PREFIX + SamplerBuilder.SAMPLER_CONF_KEY, samplerStr);
    if (samplerStr.equals("ProbabilitySampler")) {
      double percentage =
          conf.getDouble("htrace.probability.sampler.percentage", 0.01d);
      conf.setDouble(
          HTRACE_CONF_PREFIX + ProbabilitySampler.SAMPLER_FRACTION_CONF_KEY,
          percentage / 100.0d);
      LOG.info("HTrace is ON for " + percentage + "% of top-level spans.");
    }
    return new SamplerBuilder(wrapHadoopConf(conf)).build();
  }

  private static HTraceConfiguration wrapHadoopConf(final Configuration conf) {
    return new HTraceConfiguration() {
      @Override
      public String get(String key) {
        return conf.get(HTRACE_CONF_PREFIX + key);
      }

      @Override
      public String get(String key, String defaultValue) {
        return conf.get(HTRACE_CONF_PREFIX + key, defaultValue);
      }
    };
  }
}


