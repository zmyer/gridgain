/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteDhtDemandedPartitionsMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * This test checks a historical rebalance, that can be restart after canceling by some reason.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
public class WalRebalanceRestartTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 16))
                .setBackups(2))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Restart rebalance manually.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testForceReassignment() throws Exception {
        restartRebalance((ignite) -> {
            IgniteFuture manualRebFut = ignite.cache(DEFAULT_CACHE_NAME).rebalance();
        }, false);
    }

    /**
     * Restart rebalance when another server joined and baseline changed.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testAnotherServerJoinedAndChengeBlt() throws Exception {
        restartRebalance((ignite) -> {
            startGrid("new_srv");

            resetBaselineTopology();
        }, true);
    }

    /**
     * Restart rebalance when another server joined.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testAnotherServerJoined() throws Exception {
        restartRebalance((ignite) -> {
            startGrid("new_srv");
        }, true);
    }

    /**
     * Restart rebalance when new cache started.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testCacheStarted() throws Exception {
        restartRebalance((ignite) -> {
            ignite.getOrCreateCache("new_" + DEFAULT_CACHE_NAME);
        }, true);
    }

    /**
     * Restart rebalance when one of suppliers leaved topology.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testStopSupplier() throws Exception {
        restartRebalance((ignite) -> {
            stopFirstFoundSupplier(ignite);
        }, true);
    }

    /**
     * Historical rebalancer does not recovery by particular partition if this partition without a history appearing
     * in cluster.  We cannot have a historical and full rebalance on same time for difference nodes in general case.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testSatrtNewNodeAndStopSupplier() throws Exception {
        restartRebalance((ignite) -> {
            startGrid("new_srv");

            resetBaselineTopology();

            stopFirstFoundSupplier(ignite);
        }, false);
    }

    /**
     * Stop supplier and start new node.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testStopSupplierAndSatrtNewNode() throws Exception {
        restartRebalance((ignite) -> {
            stopFirstFoundSupplier(ignite);

            startGrid("new_srv");

            resetBaselineTopology();
        }, true);
    }

    /**
     * Stop first found supplier for current rebalance on specific node.
     *
     * @param ignite Ignite.
     */
    private void stopFirstFoundSupplier(IgniteEx ignite) {
        IgniteInternalFuture rebFut = ignite.cachex(DEFAULT_CACHE_NAME).context().preloader().rebalanceFuture();

        assertFalse(rebFut.isDone());

        Map<UUID, IgniteDhtDemandedPartitionsMap> remainding = U.field(rebFut, "remaining");

        assertFalse(remainding.isEmpty());

        UUID supplierId = remainding.keySet().iterator().next();

        info("First dupplier: " + supplierId);

        for (Ignite ign : G.allGrids()) {
            if (ign.cluster().localNode().id().equals(supplierId))
                ign.close();
        }
    }

    /**
     * Method hangs a rebalance on one node and invoke some trigger and check influence.
     *
     * @param retrigger Rebalance trigger.
     * @param retriggerAsHistorical True means rebalance will be restarted as historical, false is as full.
     * @throws Exception if failed.
     */
    private void restartRebalance(RebalanceRetrigger retrigger, boolean retriggerAsHistorical) throws Exception {
        IgniteEx ignite0 = startGrids(4);

        ignite0.cluster().active(true);

        try (IgniteDataStreamer streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < 1000; i++)
                streamer.addData(i, String.valueOf(i));
        }

        awaitPartitionMapExchange();
        forceCheckpoint();

        ignite(2).close();

        try (IgniteDataStreamer streamer = ignite0.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 1000; i < 2000; i++)
                streamer.addData(i, String.valueOf(i));
        }

        awaitPartitionMapExchange();
        forceCheckpoint();

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(2));

        TestRecordingCommunicationSpi spi2 = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        AtomicBoolean hasFullRebalance = new AtomicBoolean();

        spi2.record((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)msg;

                if (CU.cacheId(DEFAULT_CACHE_NAME) == demandMsg.groupId() && !F.isEmpty(demandMsg.partitions().fullSet()))
                    hasFullRebalance.compareAndSet(false, true);

            }

            return false;
        });

        spi2.blockMessages((node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage) {
                GridDhtPartitionDemandMessage demandMsg = (GridDhtPartitionDemandMessage)msg;

                if (CU.cacheId(DEFAULT_CACHE_NAME) == demandMsg.groupId())
                    return true;
            }

            return false;

        });

        IgniteEx ignite2 = startGrid(optimize(cfg));

        spi2.waitForBlocked();

        assertFalse(hasFullRebalance.get());

        retrigger.trigger(ignite2);

        spi2.stopBlock();

        awaitPartitionMapExchange();

        if (retriggerAsHistorical)
            assertFalse(hasFullRebalance.get());
        else
            assertTrue(hasFullRebalance.get());
    }

    /**
     * Rebalance trigger interface.
     */
    private static interface RebalanceRetrigger {
        public void trigger(IgniteEx ignite) throws Exception;
    }
}
