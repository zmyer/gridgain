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

import java.util.List;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class GridQueryOptimizerSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Ignite. */
    private static IgniteEx ignite;

    /** Cache. */
    private static IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = (IgniteEx)startGrid();
        cache = ignite.getOrCreateCache(new CacheConfiguration<>(CACHE_NAME));

        prepare();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    @Test
    public void testSelectExpressionSimpleCase() {
        String outerSqlTemplate = "select e.name, (%s) from emp e";
        String subSql = "select d.name from dep d where d.id = dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 1);
    }

    @Test
    public void testSelectExpressionCompoundPk() {
        String outerSqlTemplate = "select e.name, (%s) from emp e";
        String subSql = "select d.name from dep2 d where d.id = dep_id and d.id2 = id";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 1);
    }

    @Test
    public void testSelectExpressionMultipleSubQueries() {
        String outerSqlTemplate = "select (%s), (%s), (%s) from emp e1";

        String subSql1 = "select 42";
        String subSql2 = "select e2.name from emp e2 where e2.id = e1.id";
        String subSql3 = "select d.name from dep d where d.id = dep_id";

        String resSql = String.format(outerSqlTemplate, subSql1, subSql2, subSql3);

        checkResults(resSql);
        checkPlan(resSql, 2);
    }

    @Test
    public void testSelectExpressionHierarchicalSubQueries() {
        String outerSqlTemplate = "select name, (%s) as lbl from emp e1";
        String subSql = "(select 'prefix ' as pref_val) " +
            "|| (select (select name from dep2 dn where dn.id = d.id and dn.id2 = d.id2) as nn " +
            "from dep2 d where id = dep_id and id2 = dep_id)";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 2);
    }

    @Test
    public void testSelectExpressionHierarchicalSubQueriesInnerOptimization() {
        String outerSqlTemplate = "select name, (%s) as lbl from emp e";
        String subSql = "select distinct (select d2.name from dep d2 where d2.id = d1.id) from dep d1 where d1.id = e.dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 2);
    }

    @Test
    public void testSubSelectExistsClause() {
        String outerSqlTemplate = "select * from emp e where exists (%s)";
        String subSql = "select id from dep d where d.name = 'dep1' and id = e.dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 1);
    }

    @Test
    public void testSubSelectNotExistsClause() {
        String outerSqlTemplate = "select * from emp e where not exists (%s)";
        String subSql = "select id from dep d where d.name = 'dep1' and id = e.dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 2);
    }

    @Test
    public void testSubSelectExistsMultipleClause() {
        String outerSqlTemplate = "select * from emp e where exists (%s)";
        String subSql = "select id from dep d where d.name = 'dep1' and id = e.dep_id";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 1);
    }

    @Test
    @Ignore("fix hierarchical EXISTS-clause inspection")
    public void testSubSelectExistsHierarchicalClause() {
        String outerSqlTemplate = "select * from emp e where exists (%s)";
        String subSql = "select 1 from dep d1 where exists (" +
            "select 1 from dep d2 where d2.id = d1.id and exists (" +
            "select 1 from dep d3 where d3.id = d2.id))";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 1);
    }

    @Test
    public void testSubSelectInClause() {
        String outerSqlTemplate = "select * from emp e where e.dep_id in (%s)";
        String subSql = "select id from dep d";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 1);
    }

    @Test
    public void testSubSelectInClauseValueRow() {
        String outerSqlTemplate = "select * from emp e where (e.dep_id, e.dep_id, '1', 2) in (%s)";
        String subSql = "select id, id, '1', 2 from dep d";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 1);
    }

    @Test
    public void testSubSelectInClauseCompoundPk() {
        String outerSqlTemplate = "select * from emp e where (dep_id, dep_id) in (%s)";
        String subSql = "select id, id2 from dep2 d";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 1);
    }

    @Test
    public void testSubSelectInClauseCompoundPk2() {
        String outerSqlTemplate = "select * from emp e where dep_id in (%s)";
        String subSql = "select id from dep2 d where id2 = 1";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 1);
    }

    @Test
    public void testSubSelectInClauseUnsuccessfullConversion() {
        String outerSqlTemplate = "select * from emp e where dep_id in (%s)";
        String subSql = "select id from dep2 d";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 2);
    }

    @Test
    public void testSubSelectInClauseUnsuccessfullConversion2() {
        String outerSqlTemplate = "select * from emp e where dep_id in (%s)";
        String subSql = "select id from dep2 d where name = 'dep1'";

        String resSql = String.format(outerSqlTemplate, subSql);

        checkResults(resSql);
        checkPlan(resSql, 2);
    }

    private void checkResults(String sql) {
        GridQueryOptimizer.OPTIMIZATIONS_ENABLED.set(false);
        List<List<?>> exp = cache.query(new SqlFieldsQuery(sql)).getAll();
        GridQueryOptimizer.OPTIMIZATIONS_ENABLED.set(true);
        List<List<?>> act = cache.query(new SqlFieldsQuery(sql)).getAll();

        Assert.assertEquals("Result set mismatch", exp, act);
    }

    private void checkPlan(String sql, int expSelectClauses) {
        GridQueryOptimizer.OPTIMIZATIONS_ENABLED.set(true);
        String plan = cache.query(new SqlFieldsQuery("explain " + sql)).getAll().get(0).get(0).toString();
        int act = countEntries(plan, "SELECT");
        Assert.assertEquals(String.format("SELECT-clause count mismatch: exp=%d, act=%d, plan=[%s]",
            expSelectClauses, act, plan), expSelectClauses, act);
    }

    private void prepare() {
        Random rnd = new Random();

        cache.query(new SqlFieldsQuery("CREATE TABLE dep (id LONG PRIMARY KEY, name VARCHAR)"));
        cache.query(new SqlFieldsQuery("CREATE TABLE dep2 (id LONG, id2 LONG, name VARCHAR, PRIMARY KEY(id, id2))"));
        cache.query(new SqlFieldsQuery("CREATE TABLE emp (id LONG PRIMARY KEY, name VARCHAR, dep_id LONG)"));

        for (int i = 0; i < 20; i++) {
            cache.query(new SqlFieldsQuery("insert into dep (id, name) values(?, ?)")
                .setArgs(i, "dep" + i));

            cache.query(new SqlFieldsQuery("insert into dep2 (id, id2, name) values(?, ?, ?)")
                .setArgs(i, i, "dep" + i));

            cache.query(new SqlFieldsQuery("insert into emp (id, name, dep_id) values(?, ?, ?)")
                .setArgs(i, "emp" + i, i < 10 ? rnd.nextInt(10) : null));
        }
    }

    /**
     * Count of entries of substring in string.
     *
     * @param where Where to search.
     * @param what What to search.
     * @return Count of entries or -1 if non is found.
     */
    private int countEntries(String where, String what) {
        return where.split(what).length - 1;
    }
}
