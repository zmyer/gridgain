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

package org.apache.ignite.internal.sql.optimizer.affinity;

import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Collection;
import java.util.Collections;

/**
 * Node denoting empty partition set.
 */
public class PartitionNoneNode implements PartitionNode {
    /** Singleton. */
    public static final PartitionNoneNode INSTANCE = new PartitionNoneNode();

    /**
     * Constructor.
     */
    private PartitionNoneNode() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionClientContext cliCtx, Object... args) {
        return Collections.emptySet();
    }

    /** {@inheritDoc} */
    @Override public int joinGroup() {
        return PartitionTableModel.GRP_NONE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionNoneNode.class, this);
    }
}
