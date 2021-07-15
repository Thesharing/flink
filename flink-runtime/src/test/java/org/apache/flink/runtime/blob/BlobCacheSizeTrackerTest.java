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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests for {@link BlobCacheSizeTracker}. */
public class BlobCacheSizeTrackerTest extends TestLogger {
    @Test
    public void testTrackerLRUUpdate() {
        BlobCacheSizeTracker tracker = new BlobCacheSizeTracker(5L);
        List<JobID> jobIds = new ArrayList<>();
        List<BlobKey> blobKeys = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            jobIds.add(new JobID());
            blobKeys.add(BlobKey.createKey(BlobKey.BlobType.PERMANENT_BLOB));
        }
        for (int i = 0; i < 5; i++) {
            tracker.add(jobIds.get(i), blobKeys.get(i), 1);
        }
        tracker.update(jobIds.get(1), blobKeys.get(1));
        tracker.update(jobIds.get(2), blobKeys.get(2));

        List<Tuple2<JobID, BlobKey>> toDelete = tracker.checkLimit(2);

        assertThat(
                toDelete,
                containsInAnyOrder(
                        Tuple2.of(jobIds.get(0), blobKeys.get(0)),
                        Tuple2.of(jobIds.get(3), blobKeys.get(3))));
    }
}
