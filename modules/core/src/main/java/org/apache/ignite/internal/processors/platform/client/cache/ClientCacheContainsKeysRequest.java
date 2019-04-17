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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.client.ClientBooleanResponse;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * ContainsKeys request.
 */
public class ClientCacheContainsKeysRequest extends ClientCacheKeysRequest {
    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientCacheContainsKeysRequest(BinaryRawReaderEx reader) {
        super(reader);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        boolean val = cache(ctx).containsKeys(keys());

        return new ClientBooleanResponse(requestId(), val);
    }
}
