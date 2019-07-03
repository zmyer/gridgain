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
package org.apache.ignite.internal.visor.metrics;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

public class MessageStatsTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    private long[] bounds;

    private List<MessageStats> resultTable;

    public MessageStatsTaskResult(List<MessageStats> resultTable, long[] bounds) {
        this.resultTable = resultTable;

        this.bounds = bounds;
    }

    public List<MessageStats> resultTable() {
        return resultTable;
    }

    public long[] bounds() {
        return bounds;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeLongArray(out, bounds);

        U.writeCollection(out, resultTable);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        bounds = U.readLongArray(in);

        resultTable = U.readList(in);
    }

    public static class MessageStats {
        /** */
        private static final long serialVersionUID = 0L;

        private String msgType;

        private double time;

        private long[] values;

        public MessageStats(String msgType, double time, long[] values) {
            this.msgType = msgType;
            this.time = time;
            this.values = values;
        }

        public String msgType() {
            return msgType;
        }

        public double time() {
            return time;
        }

        public long[] values() {
            return values;
        }
    }
}
