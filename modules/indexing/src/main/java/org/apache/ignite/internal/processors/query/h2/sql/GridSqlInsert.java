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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.List;

/** */
public class GridSqlInsert extends GridSqlStatement {
    /** */
    private GridSqlElement into;

    /** */
    private GridSqlColumn[] cols;

    /** */
    private List<GridSqlElement[]> rows;

    /** Insert subquery. */
    private GridSqlQuery qry;

    /**
     * Not supported, introduced for clarity and correct SQL generation.
     * @see org.h2.command.dml.Insert#insertFromSelect
     */
    private boolean direct;

    /**
     * Not supported, introduced for clarity and correct SQL generation.
     * @see org.h2.command.dml.Insert#sortedInsertMode
     */
    private boolean sorted;

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StringBuilder buff = new StringBuilder(explain() ? "EXPLAIN " : "");
        buff.append("INSERT")
            .append("\nINTO ")
            .append(into.getSQL())
            .append('(');

        for (int i = 0; i < cols.length; i++) {
            GridSqlColumn col = cols[i];

            if (i > 0)
                buff.append(", ");

            buff.append('\n')
                .append(col.getSQL());
        }
        buff.append("\n)\n");

        if (direct)
            buff.append("DIRECT ");

        if (sorted)
            buff.append("SORTED ");

        if (!rows.isEmpty()) {
            buff.append("VALUES\n");
            StringBuilder valuesBuff = new StringBuilder();

            List<GridSqlElement[]> rows = rows();

            for (int i = 0; i < rows.size(); i++) {
                GridSqlElement[] row = rows.get(i);

                if (i > 0)
                    valuesBuff.append(",\n");

                StringBuilder rowBuff = new StringBuilder("(");
                for (int j = 0; j < row.length; j++) {
                    GridSqlElement e = row[j];

                    if (j > 0)
                        rowBuff.append(", ");

                    rowBuff.append(e != null ? e.getSQL() : "DEFAULT");
                }
                rowBuff.append(')');
                valuesBuff.append(rowBuff.toString());
            }
            buff.append(valuesBuff.toString());
        }
        else
            buff.append('\n')
                .append(qry.getSQL());

        return buff.toString();
    }

    /** */
    public GridSqlElement into() {
        return into;
    }

    /** */
    public GridSqlInsert into(GridSqlElement from) {
        this.into = from;
        return this;
    }

    /** */
    public List<GridSqlElement[]> rows() {
        return rows;
    }

    /** */
    public GridSqlInsert rows(List<GridSqlElement[]> rows) {
        assert rows != null;
        this.rows = rows;
        return this;
    }

    /** */
    public GridSqlQuery query() {
        return qry;
    }

    /** */
    public GridSqlInsert query(GridSqlQuery qry) {
        this.qry = qry;
        return this;
    }

    /** */
    public GridSqlColumn[] columns() {
        return cols;
    }

    /** */
    public GridSqlInsert columns(GridSqlColumn[] cols) {
        this.cols = cols;
        return this;
    }

    /** */
    public GridSqlInsert direct(boolean direct) {
        this.direct = direct;
        return this;
    }

    /** */
    public GridSqlInsert sorted(boolean sorted) {
        this.sorted = sorted;
        return this;
    }
}
