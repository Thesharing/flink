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

package org.apache.flink.runtime.topology;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.DistributionPattern;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Group that contains vertices and results. */
public class Group<E> {
    private final List<E> items;
    private final DistributionPattern distributionPattern;

    @VisibleForTesting
    public Group(List<E> items) {
        this(items, null);
    }

    @VisibleForTesting
    public Group(E item) {
        this(Collections.singletonList(item));
    }

    public Group(DistributionPattern distributionPattern) {
        this(new ArrayList<>(), distributionPattern);
    }

    public Group(List<E> items, DistributionPattern distributionPattern) {
        this.items = items;
        this.distributionPattern = distributionPattern;
    }

    public Group(E item, DistributionPattern distributionPattern) {
        this(Collections.singletonList(item), distributionPattern);
    }

    public List<E> getItems() {
        return items;
    }

    public DistributionPattern getDistributionPattern() {
        return distributionPattern;
    }
}
