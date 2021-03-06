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

package org.apache.ignite.internal.processors.cache.tree.mvcc.search;

import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * MVCC search row which uses specific MVCC version (coordinator + counter) to filter rows.
 */
public class MvccSearchRow extends SearchRow {
    /** Coordinator version. */
    protected long crdVer;

    /** Counter. */
    private long cntr;

    /** Operation counter. */
    protected int opCntr;

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     * @param crdVer Coordinator version.
     * @param cntr Counter.
     */
    public MvccSearchRow(int cacheId, KeyCacheObject key, long crdVer, long cntr, int opCntr) {
        super(cacheId, key);

        this.crdVer = crdVer;
        this.cntr = cntr;
        this.opCntr = opCntr;
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return crdVer;
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return cntr;
    }

    /** {@inheritDoc} */
    @Override public int mvccOperationCounter() {
        return opCntr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccSearchRow.class, this);
    }
}
