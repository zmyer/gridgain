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

package org.apache.ignite.internal.processors.query.h2.twostep.msg;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueCollectionBase;

import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory.fillArray;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory.toMessage;

/**
 * H2 Array.
 */
public class GridH2Array extends GridH2ValueMessage {
    /** */
    @GridDirectCollection(Message.class)
    private Collection<Message> x;

    /**
     *
     */
    public GridH2Array() {
        // No-op.
    }

    /**
     * @param val Value.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2Array(Value val) throws IgniteCheckedException {
        // Intentionally converts Value.ROW to GridH2Array to preserve compatibility
        assert val.getValueType() == Value.ARRAY || val.getValueType() == Value.ROW : val.getType();

        ValueCollectionBase col = (ValueCollectionBase)val;

        x = new ArrayList<>(col.getList().length);

        for (Value v : col.getList())
            x.add(toMessage(v));
    }

    /** {@inheritDoc} */
    @Override public Value value(GridKernalContext ctx) throws IgniteCheckedException {
        return ValueArray.get(fillArray(x.iterator(), new Value[x.size()], ctx));
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection("x", x, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 0:
                x = reader.readCollection("x", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridH2Array.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -18;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return String.valueOf(x);
    }
}
