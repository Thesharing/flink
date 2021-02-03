/*
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
 * limitations under the License
 */

package org.apache.flink.test.runtime.performance.scheduler;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

/** Utils to output memory usage. */
public class MemoryUtil {

    private static final int FACTOR = 1024 * 1024;

    public static MemoryUsage getHeapMemory() {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        memoryMXBean.gc();
        return memoryMXBean.getHeapMemoryUsage();
    }

    public static float convertToMiB(long bytes) {
        return (float) (bytes) / FACTOR;
    }

    public static String outputUsedHeapMemory(MemoryUsage memoryUsage) {
        return String.format(
                "Init: %f, Used: %f, Committed: %f, Max: %f.",
                convertToMiB(memoryUsage.getInit()),
                convertToMiB(memoryUsage.getUsed()),
                convertToMiB(memoryUsage.getCommitted()),
                convertToMiB(memoryUsage.getMax()));
    }
}
