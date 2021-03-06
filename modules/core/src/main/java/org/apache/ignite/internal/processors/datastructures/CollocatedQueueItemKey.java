/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 *
 */
public class CollocatedQueueItemKey implements QueueItemKey {
    /** */
    private IgniteUuid queueId;

    /** */
    @AffinityKeyMapped
    private int queueNameHash;

    /** */
    private long idx;

    /**
     * @param queueId Queue unique ID.
     * @param queueName Queue name.
     * @param idx Item index.
     */
    public CollocatedQueueItemKey(IgniteUuid queueId, String queueName, long idx) {
        this.queueId = queueId;
        this.queueNameHash = queueName.hashCode();
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        CollocatedQueueItemKey itemKey = (CollocatedQueueItemKey)o;

        return idx == itemKey.idx && queueId.equals(itemKey.queueId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = queueId.hashCode();

        res = 31 * res + (int)(idx ^ (idx >>> 32));

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CollocatedQueueItemKey.class, this);
    }
}
