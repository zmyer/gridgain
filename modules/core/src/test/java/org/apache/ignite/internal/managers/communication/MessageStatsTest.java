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
package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.DiagnosticMXBeanImpl;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.mxbean.DiagnosticMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MESSAGE_STATS_ENABLED;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@WithSystemProperty(key = IGNITE_MESSAGE_STATS_ENABLED, value = "true")
public class MessageStatsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        cfg.setMetricExporterSpi(new JmxMetricExporterSpi());

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testStats() throws Exception {
        startGrids(2);

        DiagnosticMXBean mxBean =
            getMxBean(grid(0).name(), "Diagnostic", DiagnosticMXBean.class, DiagnosticMXBeanImpl.class);

        assertTrue(mxBean.getDiagnosticMessageStatsEnabled());

        int testTooLongProcessing = 3725;

        mxBean.setDiagnosticMessageStatTooLongProcessing(testTooLongProcessing);

        Ignite newSrv = startGrid();

        DiagnosticMXBean newSrvMxBean =
            getMxBean(newSrv.name(), "Diagnostic", DiagnosticMXBean.class, DiagnosticMXBeanImpl.class);

        assertEquals(testTooLongProcessing, newSrvMxBean.getDiagnosticMessageStatTooLongProcessing());
    }

    /**
     *
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                //if (slowPrepare && msg0 instanceof GridNearTxPrepareRequest)
                //    doSleep(SYSTEM_DELAY);
            }

            super.sendMessage(node, msg, ackClosure);
        }
    }
}
