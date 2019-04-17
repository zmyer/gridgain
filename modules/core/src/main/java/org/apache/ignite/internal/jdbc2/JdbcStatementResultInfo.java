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

package org.apache.ignite.internal.jdbc2;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * JDBC statement result information. Keeps statement type (SELECT or UPDATE) and
 * queryId or update count (depends on statement type).
 */
public class JdbcStatementResultInfo {
    /** Query flag. */
    private boolean isQuery;

    /** Update count. */
    private long updCnt;

    /** Query ID. */
    private UUID qryId;

    /**
     * @param isQuery Query flag.
     * @param qryId Query ID.
     * @param updCnt Update count.
     */
    public JdbcStatementResultInfo(boolean isQuery, UUID qryId, long updCnt) {
        this.isQuery = isQuery;
        this.updCnt = updCnt;
        this.qryId = qryId;
    }

    /**
     * @return Query flag.
     */
    public boolean isQuery() {
        return isQuery;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return qryId;
    }

    /**
     * @return Update count.
     */
    public long updateCount() {
        return updCnt;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcStatementResultInfo.class, this);
    }
}
