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

package org.apache.ignite.internal.commandline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.baseline.BaselineArguments;
import org.apache.ignite.internal.commandline.cache.CacheCommands;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.commandline.cache.CacheValidateIndexes;
import org.apache.ignite.internal.commandline.cache.FindAndDeleteGarbage;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.commandline.management.ManagementArguments;
import org.apache.ignite.internal.commandline.management.ManagementCommands;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.tx.VisorTxOperation;
import org.apache.ignite.internal.visor.tx.VisorTxProjection;
import org.apache.ignite.internal.visor.tx.VisorTxSortOrder;
import org.apache.ignite.internal.visor.tx.VisorTxTaskArg;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.SystemPropertiesRule;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.Nullable;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.CommandList.CLUSTER_CHANGE_TAG;
import static org.apache.ignite.internal.commandline.CommandList.WAL;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_VERBOSE;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_HOST;
import static org.apache.ignite.internal.commandline.TaskExecutor.DFLT_PORT;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_DELETE;
import static org.apache.ignite.internal.commandline.WalCommands.WAL_PRINT;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.FIND_AND_DELETE_GARBAGE;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.VALIDATE_INDEXES;
import static org.apache.ignite.internal.commandline.cache.PartitionReconciliation.PARALLELISM_FORMAT_MESSAGE;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_FIRST;
import static org.apache.ignite.internal.commandline.cache.argument.ValidateIndexesCommandArg.CHECK_THROUGH;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests Command Handler parsing arguments.
 */
@WithSystemProperty(key = IGNITE_ENABLE_EXPERIMENTAL_COMMAND, value = "true")
public class CommandHandlerParsingTest {
    /** */
    @ClassRule public static final TestRule classRule = new SystemPropertiesRule();

    /** */
    @Rule public final TestRule methodRule = new SystemPropertiesRule();

    /**
     * validate_indexes command arguments parsing and validation
     */
    @Test
    public void testValidateIndexArguments() {
        //happy case for all parameters
        try {
            int expectedCheckFirst = 10;
            int expectedCheckThrough = 11;
            UUID nodeId = UUID.randomUUID();

            ConnectionAndSslParameters args = parseArgs(asList(
                CACHE.text(),
                VALIDATE_INDEXES.text(),
                "cache1, cache2",
                nodeId.toString(),
                CHECK_FIRST.toString(),
                Integer.toString(expectedCheckFirst),
                CHECK_THROUGH.toString(),
                Integer.toString(expectedCheckThrough)
            ));

            assertTrue(args.command() instanceof CacheCommands);

            CacheSubcommands subcommand = ((CacheCommands)args.command()).arg();

            CacheValidateIndexes.Arguments arg = (CacheValidateIndexes.Arguments)subcommand.subcommand().arg();

            assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeId());
            assertEquals("checkFirst parameter unexpected value", expectedCheckFirst, arg.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedCheckThrough, arg.checkThrough());
        }
        catch (IllegalArgumentException e) {
            fail("Unexpected exception: " + e);
        }

        try {
            int expectedParam = 11;
            UUID nodeId = UUID.randomUUID();

            ConnectionAndSslParameters args = parseArgs(asList(
                    CACHE.text(),
                    VALIDATE_INDEXES.text(),
                    nodeId.toString(),
                    CHECK_THROUGH.toString(),
                    Integer.toString(expectedParam)
                ));

            assertTrue(args.command() instanceof CacheCommands);

            CacheSubcommands subcommand = ((CacheCommands)args.command()).arg();

            CacheValidateIndexes.Arguments arg = (CacheValidateIndexes.Arguments)subcommand.subcommand().arg();

            assertNull("caches weren't specified, null value expected", arg.caches());
            assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeId());
            assertEquals("checkFirst parameter unexpected value", -1, arg.checkFirst());
            assertEquals("checkThrough parameter unexpected value", expectedParam, arg.checkThrough());
        }
        catch (IllegalArgumentException e) {
            e.printStackTrace();
        }

        assertParseArgsThrows("Value for '--check-first' property should be positive.", CACHE.text(), VALIDATE_INDEXES.text(), CHECK_FIRST.toString(), "0");
        assertParseArgsThrows("Numeric value for '--check-through' parameter expected.", CACHE.text(), VALIDATE_INDEXES.text(), CHECK_THROUGH.toString());
    }

    /** */
    @Test
    public void testFindAndDeleteGarbage() {
        String nodeId = UUID.randomUUID().toString();
        String delete = FindAndDeleteGarbageArg.DELETE.toString();
        String groups = "group1,grpoup2,group3";

        List<List<String>> lists = generateArgumentList(
            FIND_AND_DELETE_GARBAGE.text(),
            new T2<>(nodeId, false),
            new T2<>(delete, false),
            new T2<>(groups, false)
        );

        for (List<String> list : lists) {
            ConnectionAndSslParameters args = parseArgs(list);

            assertTrue(args.command() instanceof CacheCommands);

            CacheSubcommands subcommand = ((CacheCommands)args.command()).arg();

            FindAndDeleteGarbage.Arguments arg = (FindAndDeleteGarbage.Arguments)subcommand.subcommand().arg();

            if (list.contains(nodeId))
                assertEquals("nodeId parameter unexpected value", nodeId, arg.nodeId().toString());
            else
                assertNull(arg.nodeId());

            assertEquals(list.contains(delete), arg.delete());

            if (list.contains(groups))
                assertEquals(3, arg.groups().size());
            else
                assertNull(arg.groups());
        }
    }

    /** */
    private List<List<String>> generateArgumentList(String subcommand, T2<String, Boolean>...optional) {
        List<List<T2<String, Boolean>>> lists = generateAllCombinations(asList(optional), (x) -> x.get2());

        ArrayList<List<String>> res = new ArrayList<>();

        ArrayList<String> empty = new ArrayList<>();

        empty.add(CACHE.text());
        empty.add(subcommand);

        res.add(empty);

        for (List<T2<String, Boolean>> list : lists) {
            ArrayList<String> arg = new ArrayList<>(empty);

            list.forEach(x -> arg.add(x.get1()));

            res.add(arg);
        }

        return res;
    }

    /** */
    private <T> List<List<T>> generateAllCombinations(List<T> source, Predicate<T> stopFunc) {
        List<List<T>> res = new ArrayList<>();

        for (int i = 0; i < source.size(); i++) {
            List<T> sourceCopy = new ArrayList<>(source);

            T removed = sourceCopy.remove(i);

            generateAllCombinations(singletonList(removed), sourceCopy, stopFunc, res);
        }

        return res;
    }

    /** */
    private <T> void generateAllCombinations(List<T> res, List<T> source, Predicate<T> stopFunc, List<List<T>> acc) {
        acc.add(res);

        if (stopFunc != null && stopFunc.test(res.get(res.size() - 1)))
            return;

        if (source.size() == 1) {
            ArrayList<T> list = new ArrayList<>(res);

            list.add(source.get(0));

            acc.add(list);

            return;
        }

        for (int i = 0; i < source.size(); i++) {
            ArrayList<T> res0 = new ArrayList<>(res);

            List<T> sourceCopy = new ArrayList<>(source);

            T removed = sourceCopy.remove(i);

            res0.add(removed);

            generateAllCombinations(res0, sourceCopy, stopFunc, acc);
        }
    }

    /**
     * Tests parsing and validation for the SSL arguments.
     */
    @Test
    public void testParseAndValidateSSLArguments() {
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

            assertParseArgsThrows("Expected SSL trust store path", "--truststore");

            ConnectionAndSslParameters args = parseArgs(asList("--keystore", "testKeystore", "--keystore-password", "testKeystorePassword", "--keystore-type", "testKeystoreType",
                "--truststore", "testTruststore", "--truststore-password", "testTruststorePassword", "--truststore-type", "testTruststoreType",
                "--ssl-key-algorithm", "testSSLKeyAlgorithm", "--ssl-protocol", "testSSLProtocol", cmd.text()));

            assertEquals("testSSLProtocol", args.sslProtocol());
            assertEquals("testSSLKeyAlgorithm", args.sslKeyAlgorithm());
            assertEquals("testKeystore", args.sslKeyStorePath());
            assertArrayEquals("testKeystorePassword".toCharArray(), args.sslKeyStorePassword());
            assertEquals("testKeystoreType", args.sslKeyStoreType());
            assertEquals("testTruststore", args.sslTrustStorePath());
            assertArrayEquals("testTruststorePassword".toCharArray(), args.sslTrustStorePassword());
            assertEquals("testTruststoreType", args.sslTrustStoreType());

            assertEquals(cmd.command(), args.command());
        }
    }

    /**
     * Tests parsing and validation for user and password arguments.
     */
    @Test
    public void testParseAndValidateUserAndPassword() {
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

            assertParseArgsThrows("Expected user name", "--user");
            assertParseArgsThrows("Expected password", "--password");

            ConnectionAndSslParameters args = parseArgs(asList("--user", "testUser", "--password", "testPass", cmd.text()));

            assertEquals("testUser", args.userName());
            assertEquals("testPass", args.password());
            assertEquals(cmd.command(), args.command());
        }
    }

    /**
     * Tests parsing and validation  of WAL commands.
     */
    @Test
    public void testParseAndValidateWalActions() {
        ConnectionAndSslParameters args = parseArgs(asList(WAL.text(), WAL_PRINT));

        assertEquals(WAL.command(), args.command());

        T2<String, String> arg = ((WalCommands)args.command()).arg();

        assertEquals(WAL_PRINT, arg.get1());

        String nodes = UUID.randomUUID().toString() + "," + UUID.randomUUID().toString();

        args = parseArgs(asList(WAL.text(), WAL_DELETE, nodes));

        arg = ((WalCommands)args.command()).arg();

        assertEquals(WAL_DELETE, arg.get1());

        assertEquals(nodes, arg.get2());

        assertParseArgsThrows("Expected arguments for " + WAL.text(), WAL.text());

        String rnd = UUID.randomUUID().toString();

        assertParseArgsThrows("Unexpected action "  + rnd + " for " + WAL.text(), WAL.text(), rnd);
    }

    /**
     * Tests that the auto confirmation flag was correctly parsed.
     */
    @Test
    public void testParseAutoConfirmationFlag() {
        for (CommandList cmd : CommandList.values()) {
            if (cmd.command().confirmationPrompt() == null)
                continue;

            ConnectionAndSslParameters args;

                if (cmd == CLUSTER_CHANGE_TAG)
                    args = parseArgs(asList(cmd.text(), "test_tag"));
                else
                    args = parseArgs(asList(cmd.text()));

            checkCommonParametersCorrectlyParsed(cmd, args, false);

            switch (cmd) {
                case DEACTIVATE: {
                    args = parseArgs(asList(cmd.text(), "--yes"));

                    checkCommonParametersCorrectlyParsed(cmd, args, true);

                    break;
                }
                case BASELINE: {
                    for (String baselineAct : asList("add", "remove", "set")) {
                        args = parseArgs(asList(cmd.text(), baselineAct, "c_id1,c_id2", "--yes"));

                        checkCommonParametersCorrectlyParsed(cmd, args, true);

                        BaselineArguments arg = ((BaselineCommand)args.command()).arg();

                        assertEquals(baselineAct, arg.getCmd().text());
                        assertEquals(new HashSet<>(asList("c_id1","c_id2")), new HashSet<>(arg.getConsistentIds()));
                    }

                    break;
                }

                case TX: {
                    args = parseArgs(asList(cmd.text(), "--xid", "xid1", "--min-duration", "10", "--kill", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmd, args, true);

                    VisorTxTaskArg txTaskArg = ((TxCommands)args.command()).arg();

                    assertEquals("xid1", txTaskArg.getXid());
                    assertEquals(10_000, txTaskArg.getMinDuration().longValue());
                    assertEquals(VisorTxOperation.KILL, txTaskArg.getOperation());

                    break;
                }

                case CLUSTER_CHANGE_TAG: {
                    args = parseArgs(asList(cmd.text(), "test_tag", "--yes"));

                    checkCommonParametersCorrectlyParsed(cmd, args, true);

                    assertEquals("test_tag", ((ClusterChangeTagCommand)args.command()).arg());

                    break;
                }

                default:
                    fail("Unknown command: " + cmd);
            }
        }
    }

    /** */
    private void checkCommonParametersCorrectlyParsed(
        CommandList cmd,
        ConnectionAndSslParameters args,
        boolean autoConfirm
    ) {
        assertEquals(cmd.command(), args.command());
        assertEquals(DFLT_HOST, args.host());
        assertEquals(DFLT_PORT, args.port());
        assertEquals(autoConfirm, args.autoConfirmation());
    }

    /**
     * Tests host and port arguments.
     * Tests connection settings arguments.
     */
    @Test
    public void testConnectionSettings() {
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

            ConnectionAndSslParameters args = parseArgs(asList(cmd.text()));

            assertEquals(cmd.command(), args.command());
            assertEquals(DFLT_HOST, args.host());
            assertEquals(DFLT_PORT, args.port());

            args = parseArgs(asList("--port", "12345", "--host", "test-host", "--ping-interval", "5000",
                "--ping-timeout", "40000", cmd.text()));

            assertEquals(cmd.command(), args.command());
            assertEquals("test-host", args.host());
            assertEquals("12345", args.port());
            assertEquals(5000, args.pingInterval());
            assertEquals(40000, args.pingTimeout());

            assertParseArgsThrows("Invalid value for port: wrong-port", "--port", "wrong-port", cmd.text());
            assertParseArgsThrows("Invalid value for ping interval: -10", "--ping-interval", "-10", cmd.text());
            assertParseArgsThrows("Invalid value for ping timeout: -20", "--ping-timeout", "-20", cmd.text());
        }
    }

    /**
     * test parsing dump transaction arguments
     */
    @Test
    public void testTransactionArguments() {
        ConnectionAndSslParameters args;

        parseArgs(asList("--tx"));

        assertParseArgsThrows("Expecting --min-duration", "--tx", "--min-duration");
        assertParseArgsThrows("Invalid value for --min-duration: -1", "--tx", "--min-duration", "-1");
        assertParseArgsThrows("Expecting --min-size", "--tx", "--min-size");
        assertParseArgsThrows("Invalid value for --min-size: -1", "--tx", "--min-size", "-1");
        assertParseArgsThrows("--label", "--tx", "--label");
        assertParseArgsThrows("Illegal regex syntax", "--tx", "--label", "tx123[");
        assertParseArgsThrows("Projection can't be used together with list of consistent ids.", "--tx", "--servers", "--nodes", "1,2,3");

        args = parseArgs(asList("--tx", "--min-duration", "120", "--min-size", "10", "--limit", "100", "--order", "SIZE", "--servers"));

        VisorTxTaskArg arg = ((TxCommands)args.command()).arg();

        assertEquals(Long.valueOf(120 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(10), arg.getMinSize());
        assertEquals(Integer.valueOf(100), arg.getLimit());
        assertEquals(VisorTxSortOrder.SIZE, arg.getSortOrder());
        assertEquals(VisorTxProjection.SERVER, arg.getProjection());

        args = parseArgs(asList("--tx", "--min-duration", "130", "--min-size", "1", "--limit", "60", "--order", "DURATION",
            "--clients"));

        arg = ((TxCommands)args.command()).arg();

        assertEquals(Long.valueOf(130 * 1000L), arg.getMinDuration());
        assertEquals(Integer.valueOf(1), arg.getMinSize());
        assertEquals(Integer.valueOf(60), arg.getLimit());
        assertEquals(VisorTxSortOrder.DURATION, arg.getSortOrder());
        assertEquals(VisorTxProjection.CLIENT, arg.getProjection());

        args = parseArgs(asList("--tx", "--nodes", "1,2,3"));

        arg = ((TxCommands)args.command()).arg();

        assertNull(arg.getProjection());
        assertEquals(asList("1", "2", "3"), arg.getConsistentIds());
    }

    /** */
    @Test
    public void testValidateIndexesNotAllowedForSystemCache() {
        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "validate_indexes", "cache1,ignite-sys-cache")),
            IllegalArgumentException.class,
            "validate_indexes not allowed for `ignite-sys-cache` cache."
        );
    }

    /** */
    @Test
    public void testIdleVerifyWithCheckCrcNotAllowedForSystemCache() {
        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "idle_verify", "--check-crc", "--cache-filter", "ALL")),
            IllegalArgumentException.class,
            "idle_verify with --check-crc and --cache-filter ALL or SYSTEM not allowed. You should remove --check-crc or change --cache-filter value."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "idle_verify", "--check-crc", "--cache-filter", "SYSTEM")),
            IllegalArgumentException.class,
            "idle_verify with --check-crc and --cache-filter ALL or SYSTEM not allowed. You should remove --check-crc or change --cache-filter value."
        );

        GridTestUtils.assertThrows(
            null,
            () -> parseArgs(asList("--cache", "idle_verify", "--check-crc", "ignite-sys-cache")),
            IllegalArgumentException.class,
            "idle_verify with --check-crc not allowed for `ignite-sys-cache` cache."
        );
    }

    /**
     * Argument validation test.
     *
     * validate that following partition_reconciliation arguments validated as expected:
     *
     * --repair
     *      if value is missing - IllegalArgumentException (The repair algorithm should be specified.
     *      The following values can be used: [LATEST, PRIMARY, MAJORITY, REMOVE, PRINT_ONLY].) is expected.
     *      if unsupported value is used - IllegalArgumentException (Invalid repair algorithm: <invalid-repair-alg>.
     *      The following values can be used: [LATEST, PRIMARY, MAJORITY, REMOVE, PRINT_ONLY].) is expected.
     *
     * --parallelism
     *      Int value from [0, 128] is expected.
     *      If value is missing of differs from metioned integer -
     *      IllegalArgumentException (Invalid parallelism) is expected.
     *
     * --batch-size
     *      if value is missing - IllegalArgumentException (The batch size should be specified.) is expected.
     *      if unsupported value is used - IllegalArgumentException (Invalid batch size: <invalid-batch-size>.
     *      Int value greater than zero should be used.) is expected.
     *
     * --recheck-attempts
     *      if value is missing - IllegalArgumentException (The recheck attempts should be specified.) is expected.
     *      if unsupported value is used - IllegalArgumentException (Invalid recheck attempts:
     *      <invalid-recheck-attempts>. Int value between 1 and 5 should be used.) is expected.
     *
     * As invalid values use values that produce NumberFormatException and out-of-range values.
     * Also ensure that in case of appropriate parameters parseArgs() doesn't throw any exceptions.
     */
    @Test
    public void testPartitionReconciliationArgumentsValidation() {
        assertParseArgsThrows("The repair algorithm should be specified. The following values can be used: "
            + Arrays.toString(RepairAlgorithm.values()) + '.', "--cache", "partition_reconciliation", "--repair");

        assertParseArgsThrows("Invalid repair algorithm: invalid-repair-alg. The following values can be used: "
            + Arrays.toString(RepairAlgorithm.values()) + '.', "--cache", "partition_reconciliation", "--repair",
            "invalid-repair-alg");

        parseArgs(asList("--cache", "partition_reconciliation", "--fix-alg", "PRIMARY"));

        // --load-factor
        assertParseArgsThrows("The parallelism level should be specified.",
            "--cache", "partition_reconciliation", "--parallelism");

        assertParseArgsThrows(String.format(PARALLELISM_FORMAT_MESSAGE, "abc"),
            "--cache", "partition_reconciliation", "--parallelism", "abc");

        assertParseArgsThrows(String.format(PARALLELISM_FORMAT_MESSAGE, "0.5"),
            "--cache", "partition_reconciliation", "--parallelism", "0.5");

        assertParseArgsThrows(String.format(PARALLELISM_FORMAT_MESSAGE, "-1"),
            "--cache", "partition_reconciliation", "--parallelism", "-1");

        parseArgs(asList("--cache", "partition_reconciliation", "--parallelism", "8"));

        parseArgs(asList("--cache", "partition_reconciliation", "--parallelism", "1"));

        parseArgs(asList("--cache", "partition_reconciliation", "--parallelism", "0"));

        // --batch-size
        assertParseArgsThrows("The batch size should be specified.",
            "--cache", "partition_reconciliation", "--batch-size");

        assertParseArgsThrows("Invalid batch size: abc. Integer value greater than zero should be used.",
            "--cache", "partition_reconciliation", "--batch-size", "abc");

        assertParseArgsThrows("Invalid batch size: 0. Integer value greater than zero should be used.",
            "--cache", "partition_reconciliation", "--batch-size", "0");

        parseArgs(asList("--cache", "partition_reconciliation", "--batch-size", "10"));

        // --recheck-attempts
        assertParseArgsThrows("The recheck attempts should be specified.",
            "--cache", "partition_reconciliation", "--recheck-attempts");

        assertParseArgsThrows("Invalid recheck attempts: abc. Integer value between 1 (inclusive) and 5 (exclusive) should be used.",
            "--cache", "partition_reconciliation", "--recheck-attempts", "abc");

        assertParseArgsThrows("Invalid recheck attempts: 6. Integer value between 1 (inclusive) and 5 (exclusive) should be used.",
            "--cache", "partition_reconciliation", "--recheck-attempts", "6");

        parseArgs(asList("--cache", "partition_reconciliation", "--recheck-attempts", "1"));

        parseArgs(asList("--cache", "partition_reconciliation", "--recheck-attempts", "5"));

        // --recheck-delay
        assertParseArgsThrows("The recheck delay should be specified.",
            "--cache", "partition_reconciliation", "--recheck-delay");

        assertParseArgsThrows("Invalid recheck delay: abc. Integer value between 0 (inclusive) and 100 (exclusive) should be used.",
            "--cache", "partition_reconciliation", "--recheck-delay", "abc");

        assertParseArgsThrows("Invalid recheck delay: 101. Integer value between 0 (inclusive) and 100 (exclusive) should be used.",
            "--cache", "partition_reconciliation", "--recheck-delay", "101");

        parseArgs(asList("--cache", "partition_reconciliation", "--recheck-delay", "0"));

        parseArgs(asList("--cache", "partition_reconciliation", "--recheck-delay", "50"));
    }

    /**
     * Test checks that option {@link CommonArgParser#CMD_VERBOSE} is parsed
     * correctly and if it is not present, it takes the default value
     * {@code false}.
     */
    @Test
    public void testParseVerboseOption() {
        for (CommandList cmd : CommandList.values()) {
            if (requireArgs(cmd))
                continue;

            assertFalse(cmd.toString(), parseArgs(singletonList(cmd.text())).verbose());
            assertTrue(cmd.toString(), parseArgs(asList(cmd.text(), CMD_VERBOSE)).verbose());
        }
    }

    /**
     * @param args Raw arg list.
     * @return Common parameters container object.
     */
    private ConnectionAndSslParameters parseArgs(List<String> args) {
        return new CommonArgParser(setupTestLogger()).
            parseAndValidate(args.iterator());
    }

    /**
     * @return logger for tests.
     */
    private Logger setupTestLogger() {
        Logger result;

        result = Logger.getLogger(getClass().getName());
        result.setLevel(Level.INFO);
        result.setUseParentHandlers(false);

        result.addHandler(CommandHandler.setupStreamHandler());

        return result;
    }

    /**
     * Checks that parse arguments fails with {@link IllegalArgumentException} and {@code failMsg} message.
     *
     * @param failMsg Exception message (optional).
     * @param args Incoming arguments.
     */
    private void assertParseArgsThrows(@Nullable String failMsg, String... args) {
        assertThrows(null, () -> parseArgs(asList(args)), IllegalArgumentException.class, failMsg);
    }

    /**
     * Return {@code True} if cmd there are required arguments.
     *
     * @return {@code True} if cmd there are required arguments.
     */
    private boolean requireArgs(@Nullable CommandList cmd) {
        return cmd == CommandList.CACHE ||
            cmd == CommandList.WAL ||
            cmd == CommandList.ROLLING_UPGRADE ||
            cmd == CommandList.CLUSTER_CHANGE_TAG ||
            cmd == CommandList.DATA_CENTER_REPLICATION ||
            cmd == CommandList.MANAGEMENT;
    }

    /**
     * test parsing management arguments
     */
    @Test
    public void testManagementArguments() throws IOException {
        ConnectionAndSslParameters args;

        parseArgs(asList("--management", "status"));
        parseArgs(asList("--management", "help"));
        
        args = parseArgs(asList("--management", "on"));

        ManagementArguments arg = ((ManagementCommands)args.command()).arg();

        assertTrue(arg.isEnable());

        args = parseArgs(asList("--management", "off"));

        arg = ((ManagementCommands)args.command()).arg();

        assertFalse(arg.isEnable());

        assertParseArgsThrows("Expected server URIs", "--management", "uri");

        args = parseArgs(asList("--management", "uri", "http://localhost:3000"));

        arg = ((ManagementCommands)args.command()).arg();
        
        assertTrue(arg.isEnable());
        assertEquals(singletonList("http://localhost:3000"), arg.getServerUris());

        assertParseArgsThrows("Invalid uri argument: --wrong", "--management", "uri", "http://localhost", "--wrong");

        String keyStorePath = U.resolveIgnitePath("/modules/core/src/test/resources/server.jks").getAbsolutePath();
        String trustStorePath = U.resolveIgnitePath("/modules/core/src/test/resources/server.jks").getAbsolutePath();

        args = parseArgs(asList("--management", "uri", "http://localhost", "--management-cipher-suites", "CIPHER_1,CIPHER_2",
            "--management-keystore", keyStorePath,
            "--management-keystore-password", "KEYSTORE_PASSWORD",
            "--management-truststore", trustStorePath,
            "--management-truststore-password", "TRUSTSTORE_PASSWORD",
            "--management-session-timeout", "1",
            "--management-session-expiration-timeout", "100"));

        arg = ((ManagementCommands)args.command()).arg();

        assertTrue(arg.isEnable());
        assertEquals(singletonList("http://localhost"), arg.getServerUris());
        assertEquals(asList("CIPHER_1", "CIPHER_2"), arg.getCipherSuites());
        assertEquals(U.readFileToString(keyStorePath, "UTF-8"), arg.getKeyStore());
        assertEquals("KEYSTORE_PASSWORD", arg.getKeyStorePassword());
        assertEquals(U.readFileToString(trustStorePath, "UTF-8"), arg.getTrustStore());
        assertEquals("TRUSTSTORE_PASSWORD", arg.getTrustStorePassword());
        assertEquals(1, arg.getSessionTimeout());
        assertEquals(100, arg.getSessionExpirationTimeout());

        assertParseArgsThrows("Expecting session timeout", "--management", "uri", "http://localhost", "--management-session-timeout");
        assertParseArgsThrows("Invalid value for session timeout: x", "--management", "uri", "http://localhost", "--management-session-timeout", "x");

        assertParseArgsThrows("Expecting session expiration timeout", "--management", "uri", "http://localhost", "--management-session-expiration-timeout");
        assertParseArgsThrows("Invalid value for session expiration timeout: x", "--management", "uri", "http://localhost", "--management-session-expiration-timeout", "x");
    }
}
