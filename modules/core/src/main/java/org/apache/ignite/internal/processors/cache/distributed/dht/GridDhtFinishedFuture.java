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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Finished DHT future.
 */
public class GridDhtFinishedFuture<T> extends GridFinishedFuture<T> implements GridDhtFuture<T> {
    /**
     * @param t Result.
     */
    public GridDhtFinishedFuture(T t) {
        super(t);
    }

    /**
     * @param err Error.
     */
    public GridDhtFinishedFuture(Throwable err) {
        super(err);
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> invalidPartitions() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtFinishedFuture.class, this, super.toString());
    }
}