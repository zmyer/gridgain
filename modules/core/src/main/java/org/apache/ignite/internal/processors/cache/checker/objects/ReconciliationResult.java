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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class ReconciliationResult extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    private PartitionReconciliationResult partitionReconciliationResult;

    /**
     *
     */
    private Map<UUID, String> nodeIdToFolder;

    /**
     *
     */
    public ReconciliationResult() {
    }

    /**
     *
     */
    public ReconciliationResult(
        PartitionReconciliationResult partReconciliationRes,
        Map<UUID, String> nodeIdToFolder
    ) {
        this.partitionReconciliationResult = partReconciliationRes;
        this.nodeIdToFolder = nodeIdToFolder;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(partitionReconciliationResult);
        U.writeMap(out, nodeIdToFolder);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        partitionReconciliationResult = (PartitionReconciliationResult)in.readObject();

        nodeIdToFolder = U.readMap(in);
    }

    /**
     *
     */
    public PartitionReconciliationResult partitionReconciliationResult() {
        return partitionReconciliationResult;
    }

    /**
     *
     */
    public Map<UUID, String> nodeIdToFolder() {
        return nodeIdToFolder;
    }
}
