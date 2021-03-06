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

package org.apache.ignite.plugin.extensions.communication;

import org.jetbrains.annotations.Nullable;

/**
 * Enum representing possible types of collection items.
 */
public enum MessageCollectionItemType {
    /** Byte. */
    BYTE,

    /** Short. */
    SHORT,

    /** Integer. */
    INT,

    /** Long. */
    LONG,

    /** Float. */
    FLOAT,

    /** Double. */
    DOUBLE,

    /** Character. */
    CHAR,

    /** Boolean. */
    BOOLEAN,

    /** Byte array. */
    BYTE_ARR,

    /** Short array. */
    SHORT_ARR,

    /** Integer array. */
    INT_ARR,

    /** Long array. */
    LONG_ARR,

    /** Float array. */
    FLOAT_ARR,

    /** Double array. */
    DOUBLE_ARR,

    /** Character array. */
    CHAR_ARR,

    /** Boolean array. */
    BOOLEAN_ARR,

    /** String. */
    STRING,

    /** Bit set. */
    BIT_SET,

    /** UUID. */
    UUID,

    /** Ignite UUID. */
    IGNITE_UUID,

    /** Message. */
    MSG,

    /** Topology version. */
    AFFINITY_TOPOLOGY_VERSION;

    /** Enum values. */
    private static final MessageCollectionItemType[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value.
     */
    @Nullable public static MessageCollectionItemType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}