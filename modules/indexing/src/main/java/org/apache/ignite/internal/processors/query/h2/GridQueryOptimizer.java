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

package org.apache.ignite.internal.processors.query.h2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2ProxyIndex;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunction;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperation;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSubquery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUnion;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlValueRow;
import org.h2.index.Index;
import org.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst.TRUE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.AND;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.EQUAL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.EXISTS;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.IN;

/**
 *
 */
public class GridQueryOptimizer {
    /** Optimizations switch. */
    static final ThreadLocal<Boolean> OPTIMIZATIONS_ENABLED = new ThreadLocal<>();
    static {
        OPTIMIZATIONS_ENABLED.set(true);
    }

    /**
     * @param parent Parent.
     */
    public static void pullOutSubQuery(GridSqlQuery parent) {
        if (!OPTIMIZATIONS_ENABLED.get())
            return;

        if (parent instanceof GridSqlUnion) {
            GridSqlUnion union = (GridSqlUnion)parent;
            pullOutSubQuery(union.left());
            pullOutSubQuery(union.right());
            return;
        }

        // pull out select expressions from SELECT clause
        GridSqlSelect select = (GridSqlSelect)parent;
        boolean wasPulledOut;
        for (int i = 0; i < select.allColumns(); i++) {
            wasPulledOut = false;
            GridSqlAst col = select.columns(false).get(i);
            if (col instanceof GridSqlSubquery) {
                GridSqlSubquery subQ = (GridSqlSubquery)col;
                if (pullOutSubQryFromSelectExpr(select, null, i))
                    wasPulledOut = true;
                else
                    pullOutSubQuery(subQ.subquery());
            }
            else {
                ASTNodeFinder finder = new ASTNodeFinder((GridSqlElement)col,
                    (parent0, child) -> child instanceof GridSqlSubquery);

                ASTNodeFinder.Result res;
                while ((res = finder.findNext()) != null) {
                    GridSqlSubquery subQ = res.getEl().child(res.getIdx());
                    if (pullOutSubQryFromSelectExpr(select, res.getEl(), res.getIdx()))
                        wasPulledOut = true;
                    else
                        pullOutSubQuery(subQ.subquery());
                }
            }

            if (wasPulledOut)
                i--;
        }

        // pull out sub-queries from IN clause
        do {
            wasPulledOut = false;

            if (select.where() instanceof GridSqlOperation && ((GridSqlOperation)select.where()).operationType() == IN
                && select.where().child(1) instanceof GridSqlSubquery) {

                GridSqlSubquery subQ = select.where().child(1);
                if (pullOutSubQryFromInClause(select, null, -1))
                    wasPulledOut = true;
                else
                    pullOutSubQuery(subQ.subquery());
            }
            else if (select.where() instanceof GridSqlOperation && ((GridSqlOperation)select.where()).operationType() == AND) {

                ASTNodeFinder finder = new ASTNodeFinder((GridSqlElement)select.where(),
                    (parent0, child) -> child instanceof GridSqlOperation
                        && ((GridSqlOperation)child).operationType() == IN && child.child(1) instanceof GridSqlSubquery,
                    child -> child instanceof GridSqlOperation && ((GridSqlOperation)child).operationType() == AND);

                ASTNodeFinder.Result res;
                while ((res = finder.findNext()) != null) {
                    GridSqlSubquery subQ = res.getEl().child(res.getIdx()).child(1);
                    if (pullOutSubQryFromInClause(select, res.getEl(), res.getIdx()))
                        wasPulledOut = true;
                    else
                        pullOutSubQuery(subQ.subquery());
                }
            }
        }
        while (wasPulledOut);

        // pull out sub-queries from EXISTS clause
        do {
            wasPulledOut = false;
            if (select.where() instanceof GridSqlOperation && ((GridSqlOperation)select.where()).operationType() == EXISTS
                && select.where().child() instanceof GridSqlSubquery) {

                GridSqlSubquery subQ = select.where().child();
                if (pullOutSubQryFromExistsClause(select, null, -1))
                    wasPulledOut = true;
                else
                    pullOutSubQuery(subQ.subquery());
            }
            else if (select.where() instanceof GridSqlOperation && ((GridSqlOperation)select.where()).operationType() == AND) {
                ASTNodeFinder finder = new ASTNodeFinder((GridSqlElement)select.where(),
                    (parent0, child) -> child instanceof GridSqlOperation
                        && ((GridSqlOperation)child).operationType() == EXISTS && child.child() instanceof GridSqlSubquery,
                    child -> child instanceof GridSqlOperation && ((GridSqlOperation)child).operationType() == AND);

                ASTNodeFinder.Result res;
                wasPulledOut = false;
                while ((res = finder.findNext()) != null) {
                    GridSqlSubquery subQ = res.getEl().child(res.getIdx()).child();
                    if (pullOutSubQryFromExistsClause(select, res.getEl(), res.getIdx()))
                        wasPulledOut = true;
                    else
                        pullOutSubQuery(subQ.subquery());
                }
            }

        }
        while (wasPulledOut);
    }

    /**
     * Whether Select query is simple or not.
     * <p>
     * We call query simple if it is select query (not union) and it has neither having nor grouping,
     * has no distinct clause, has no aggregations, has no limits, no sorting, no offset clause.
     * Also it is not SELECT FOR UPDATE.
     *
     * @param subQry Sub query.
     * @return {@code true} if it is simple query.
     */
    private static boolean isSimpleSelect(GridSqlQuery subQry) {
        if (subQry instanceof GridSqlUnion)
            return false;

        // TODO: check for any aggregates
        GridSqlSelect select = (GridSqlSelect)subQry;
        return (select.sort() == null || select.sort().isEmpty())
            && select.offset() == null
            && select.limit() == null
            && !select.isForUpdate()
            && !select.distinct()
            && GridSqlAlias.unwrap(select.from()) instanceof GridSqlTable
            && select.havingColumn() < 0
            && (select.groupColumns() == null || select.groupColumns().length <= 0);
    }


    /**
     * Check whether table have unique index that can be built with provided column set.
     *
     * @param gridCols Columns.
     * @param tbl Table.
     * @return {@code true} if there is the unique index.
     */
    private static boolean isUniqueIndexExists(Collection<GridSqlColumn> gridCols, GridSqlTable tbl) {
        Set<Column> cols = gridCols.stream().map(GridSqlColumn::column).collect(Collectors.toSet());

        for (Index idx : tbl.dataTable().getIndexes()) {
            // if index is unique
            if ((idx.getIndexType().isUnique()
                // or index is a proxy to unique index
                || (idx instanceof GridH2ProxyIndex && ((GridH2ProxyIndex)idx).underlyingIndex().getIndexType().isUnique()))
                // and indexed can be build with provided columns
                && cols.containsAll(Arrays.asList(idx.getColumns())))

                return true;
        }

        return false;
    }

    /**
     * Pull out sub-select from SELECT clause to the parent select level.
     * <p>
     * Example:
     * <pre>
     *   Before:
     *     SELECT name,
     *            (SELECT name FROM dep d WHERE d.id = e.dep_id) as dep_name
     *       FROM emp e
     *      WHERE sal > 2000
     *
     *   After:
     *     SELECT name,
     *            d.name as dep_name
     *       FROM emp e
     *       LEFT JOIN dep d
     *         ON d.id = e.dep_id
     *      WHERE sal > 2000
     * </pre>
     *
     * @param parent Parent select.
     * @param targetEl Target sql element. Can be null.
     * @param childInd Column ind.
     */
    private static boolean pullOutSubQryFromSelectExpr(GridSqlSelect parent,
        @Nullable GridSqlElement targetEl, int childInd) {

        GridSqlSubquery subQry = targetEl != null
            ? targetEl.child(childInd)
            : (GridSqlSubquery)parent.columns(false).get(childInd);

        GridSqlSelect subS = testSubQueryCanBePulledOut(subQry);
        if (subS == null)
            return false;

        if (subS.allColumns() != 1)
            return false;

        GridSqlAst subCol = GridSqlAlias.unwrap(subS.columns(false).get(0));

        if (targetEl != null)
            targetEl.child(childInd, subCol);
        else
            parent.setColumn(childInd, subCol);

        GridSqlElement parentFrom = parent.from() instanceof GridSqlElement
            ? (GridSqlElement)parent.from()
            : new GridSqlSubquery((GridSqlQuery)parent.from());

        parent.from(new GridSqlJoin(parentFrom, GridSqlAlias.unwrap(subS.from()),
            true, (GridSqlElement)subS.where())).child();

        return true;
    }

    /**
     * Pull out sub-select from IN clause to the parent select level.
     * <p>
     * Example:
     * <pre>
     *   Before:
     *     SELECT name
     *       FROM emp e
     *      WHERE e.dep_id IN (SELECT d.id FROM dep d WHERE d.name = 'dep1')
     *        AND sal > 2000
     *
     *   After:
     *     SELECT name
     *       FROM emp e
     *       JOIN dep d
     *         ON d.id = e.dep_id and d.name = 'dep1
     *      WHERE sal > 2000
     * </pre>
     *
     * @param parent Parent select.
     * @param targetEl Target sql element. Can be null.
     * @param childInd Column ind.
     */
    private static boolean pullOutSubQryFromInClause(GridSqlSelect parent, @Nullable GridSqlElement targetEl,
        int childInd) {
        // extract sub-query
        GridSqlSubquery subQry = targetEl != null
            ? targetEl.child(childInd).child(1)
            : parent.where().child(1);

        if (!isSimpleSelect(subQry.subquery()))
            return false;

        GridSqlSelect subS = subQry.subquery();

        GridSqlAst targetExpr = targetEl != null
            ? targetEl.child(childInd).child(0)
            : parent.where().child(0);

        // should be allowed only following variation:
        //      1) tbl_col IN (SELECT in_col FROM ...)
        //      2) (c1, c2, ..., cn) IN (SELECT c1, c2, ..., cn FROM ...)
        //      3) const IN (SELECT in_col FROM ...)
        //      4) c1 + c2/const IN (SELECT in_col FROM ...)
        if (targetExpr instanceof GridSqlValueRow) {
            if (targetExpr.size() != subS.visibleColumns())
                return false;
        }
        else if (targetEl instanceof GridSqlFunction)
            return false;
        else if (subS.visibleColumns() != 1)
            return false;

        List<GridSqlElement> conditions = new ArrayList<>();
        for (int i = 0; i < subS.visibleColumns(); i++) {
            GridSqlElement el = targetExpr instanceof GridSqlValueRow ? targetExpr.child(i) : (GridSqlElement)targetExpr;
            conditions.add(new GridSqlOperation(EQUAL, el, subS.columns(true).get(i)));
        }

        // save old condition and IN expression to restore them
        // in case of unsuccessfull pull out
        GridSqlElement oldCond = (GridSqlElement)subS.where();
        if (oldCond != null)
            conditions.add(oldCond);

        GridSqlElement oldInExpr = targetEl != null
            ? targetEl.child(childInd)
            : (GridSqlElement)parent.where();

        // update sub-query condition and convert IN clause to EXISTS
        subS.where(buildConditionBush(conditions));
        GridSqlOperation existsExpr = new GridSqlOperation(EXISTS, subQry);
        if (targetEl != null)
            targetEl.child(childInd, existsExpr);
        else
            parent.where(existsExpr);

        boolean res = pullOutSubQryFromExistsClause(parent, targetEl, childInd);

        if (!res) { // restore original query in case of failure
            if (targetEl != null)
                targetEl.child(childInd, oldInExpr);
            else
                parent.where(oldInExpr);

            subS.where(oldCond);
        }

        return res;
    }

    /**
     * @param ops Ops.
     */
    private static GridSqlElement buildConditionBush(List<GridSqlElement> ops) {
        assert ops != null && !ops.isEmpty();
        if (ops.size() == 1)
            return ops.get(0);

        int m = (ops.size() + 1) / 2;
        GridSqlElement left = buildConditionBush(ops.subList(0, m));
        GridSqlElement right = buildConditionBush(ops.subList(m, ops.size()));

        return new GridSqlOperation(AND, left, right);
    }


    /**
     * Pull out sub-select from EXISTS clause to the parent select level.
     * <p>
     * Example:
     * <pre>
     *   Before:
     *     SELECT name
     *       FROM emp e
     *      WHERE EXISTS (SELECT 1 FROM dep d WHERE d.id = e.dep_id and d.name = 'dep1')
     *        AND sal > 2000
     *
     *   After:
     *     SELECT name
     *       FROM emp e
     *       JOIN dep d
     *         ON d.id = e.dep_id and d.name = 'dep1
     *      WHERE sal > 2000
     * </pre>
     *
     * @param parent Parent select.
     * @param targetEl Target sql element. Can be null.
     * @param childInd Column ind.
     */
    private static boolean pullOutSubQryFromExistsClause(GridSqlSelect parent, @Nullable GridSqlElement targetEl,
        int childInd) {

        // extract sub-query
        GridSqlSubquery subQry = targetEl != null
            ? targetEl.child(childInd).child()
            : parent.where().child();

        GridSqlSelect subS = testSubQueryCanBePulledOut(subQry);
        if (subS == null)
            return false;

        if (targetEl != null)
            // may be will be better to replace entire branch
            targetEl.child(childInd, TRUE);
        else
            parent.where(null);

        GridSqlElement parentFrom = parent.from() instanceof GridSqlElement
            ? (GridSqlElement)parent.from()
            : new GridSqlSubquery((GridSqlQuery)parent.from());

        parent.from(new GridSqlJoin(parentFrom, GridSqlAlias.unwrap(subS.from()),
            false, (GridSqlElement)subS.where())).child();

        return true;
    }

    /**
     * Whether sub-query can be pulled out or not.
     *
     * @param subQry Sub-query.
     * @return Sub-query if it can be pulled out or {@null}.
     */
    private static GridSqlSelect testSubQueryCanBePulledOut(GridSqlSubquery subQry) {

        GridSqlQuery subQ = subQry.subquery();

        if (!isSimpleSelect(subQ))
            return null;

        assert subQ instanceof GridSqlSelect;

        GridSqlSelect subS = (GridSqlSelect)subQ;

        // ensure that sub-query returns at most one row
        // for now we say, that it is true when there is set of columns such:
        //      1) column from sub-query table
        //      2) can be accessible by any AND branch of the WHERE tree:
        //                  WHERE
        //                    |
        //                   AND
        //                  /    \
        //                AND    OR
        //               /   \   / \
        //              c1   c2 *   * // * - can't be reached by AND branch
        //
        //      2) is strict predicate to sub-query table (only equality supported)
        //      3) this set is superset for any of unique index, e.g. for query like
        //                 WHERE c1 = v1
        //                   AND c2 = v2
        //                         ...
        //                   AND ci = vi
        //                   AND ci+1 = vi+1
        //                   AND ci+2 = vi+2,
        //         there is unique index unique_idx(c1, c2, ..., ci)

        // TODO: think about more fair check
        if (subS.where() == null)
            return null;

        ASTNodeFinder finder = new ASTNodeFinder((GridSqlElement)subS.where(),
            (parent0, child) -> parent0 instanceof GridSqlColumn,
            child -> {
                if (child instanceof GridSqlColumn)
                    return true;

                if (!(child instanceof GridSqlOperation))
                    return false;

                GridSqlOperationType opType = ((GridSqlOperation)child).operationType();
                switch (opType) {
                    case AND:
                    case EQUAL:
                    case EQUAL_NULL_SAFE:
                    case NEGATE:
                        return true;

                    default:
                        /* NO-OP */
                        break;
                }

                return false;
            });

        List<GridSqlColumn> sqlCols = new ArrayList<>();
        ASTNodeFinder.Result res;
        while ((res = finder.findNext()) != null)
            sqlCols.add((GridSqlColumn)res.getEl());

        GridSqlTable subTbl = GridSqlAlias.unwrap(subS.from());
        return isUniqueIndexExists(sqlCols, subTbl) ? subS : null;
    }
}

/**
 * Abstract syntax tree node finder.
 * Finds all nodes that satisfies the {@link ASTNodeFinder#emitPred} in depth-first manner.
 */
class ASTNodeFinder {
    /**
     * Tree node iterator.
     */
    private class NodeIterator {
        /** Index of the current child. */
        private int childInd;
        /** Element on whose children iterate. */
        private final GridSqlElement el;

        /**
         * @param el El.
         */
        private NodeIterator(GridSqlElement el) {
            this.childInd = -1;
            this.el = el;
        }

        /**
         * Returns next child.
         * @return Next child or {@code null}, if there are no more children.
         */
        GridSqlAst nextChild() {
            if (++childInd < el.size())
                return el.child(childInd);

            return null;
        }
    }

    /**
     * Result of the iteration.
     */
    public static class Result {
        /** Index of the child which satisfies the emit predicate. */
        private final int idx;
        /** Element on whose children iterate. */
        private final GridSqlElement el;

        /**
         * @param idx Index.
         * @param el El.
         */
        private Result(int idx, GridSqlElement el) {
            this.idx = idx;
            this.el = el;
        }

        /**
         * Returns index of the child.
         * @return index of the child.
         */
        public int getIdx() {
            return idx;
        }

        /**
         * Returns parent element.
         * @return parent element.
         */
        public GridSqlElement getEl() {
            return el;
        }
    }

    /** Stack of the tree node. */
    private final List<NodeIterator> nodeStack = new ArrayList<>();
    /** Emit predicate. */
    private final BiPredicate<GridSqlAst, GridSqlAst> emitPred;
    /** Walk predicate. */
    private final Predicate<GridSqlAst> walkPred;

    /**
     * @param root Root.
     * @param emitPred Emit predicate. Used to decide whether the current
     *                 should be returned by {@link ASTNodeFinder#findNext()} or not.
     * @param walkPred Walk predicate. Used to decide should finder step into this node or just skip it.
     */
    ASTNodeFinder(GridSqlElement root,
        BiPredicate<GridSqlAst, GridSqlAst> emitPred,
        Predicate<GridSqlAst> walkPred) {

        assert root != null : "root";
        assert emitPred != null : "emitPredicate";
        assert walkPred != null : "walkPred";

        this.emitPred = emitPred;
        this.walkPred = walkPred;

        nodeStack.add(new NodeIterator(root));
    }

    /**
     * @param root Root.
     * @param emitPred Emit predicate. Used to decide whether the current
     *                 should be returned by {@link ASTNodeFinder#findNext()} or not.
     */
    ASTNodeFinder(GridSqlElement root,
        BiPredicate<GridSqlAst, GridSqlAst> emitPred) {
        this.emitPred = emitPred;
        this.walkPred = child -> child instanceof GridSqlElement;

        nodeStack.add(new NodeIterator(root));
    }

    /**
     * Returns next result that satisfies the {@link ASTNodeFinder#emitPred}
     * @return next result or null, if there are no more nodes.
     */
    Result findNext() {
        while (!nodeStack.isEmpty()) {
            NodeIterator curNode = nodeStack.get(nodeStack.size() - 1);

            GridSqlAst curChild = curNode.nextChild();
            if (curChild == null)
                nodeStack.remove(nodeStack.size() - 1);

            if (emitPred.test(curNode.el, curChild))
                return new Result(curNode.childInd, curNode.el);

            if (walkPred.test(curChild))
                nodeStack.add(new NodeIterator((GridSqlElement)curChild));
        }

        return null;
    }
}
