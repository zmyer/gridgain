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

package org.gridgain.service;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.metric.export.MetricRequest;
import org.apache.ignite.internal.processors.metric.export.MetricResponse;
import org.gridgain.agent.WebSocketManager;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.util.MimeTypeUtils;

import java.util.Collection;
import java.util.UUID;

import static org.apache.ignite.internal.GridTopic.TOPIC_METRICS;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.gridgain.agent.StompDestinationsUtils.buildMetricsDest;

/**
 * Metric service.
 */
public class MetricsService implements AutoCloseable {
    /** Context. */
    private GridKernalContext ctx;

    /** Manager. */
    private WebSocketManager mgr;

    /** Logger. */
    private IgniteLogger log;

    private GridMessageListener lsnr = this::onNodeMetrics;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public MetricsService(GridKernalContext ctx, WebSocketManager mgr) {
        this.ctx = ctx;
        this.mgr = mgr;
        this.log = ctx.log(MetricsService.class);

        // Listener for collecting metrics event.
        ctx.io().addMessageListener(TOPIC_METRICS, lsnr);
    }

    /**
     * Process node metrics message.
     *
     * @param nodeId ID of node that sent the message. Note that may have already
     *      left topology by the time this message is received.
     * @param msg Message received.
     * @param plc Message policy (pool).
     */
     void onNodeMetrics(UUID nodeId, Object msg, byte plc) {
        if (msg instanceof MetricResponse) {
            MetricResponse res = (MetricResponse)msg;

            // TODO GG-22191 change on debug level.
            log.info("Send message to GMC: " + msg);

            try {
                // TODO: workaround of spring-messaging bug with send byte array data.
                // https://github.com/spring-projects/spring-framework/issues/23358
                StompHeaders headers = new StompHeaders();
                headers.setContentType(MimeTypeUtils.APPLICATION_OCTET_STREAM);
                headers.setDestination(buildMetricsDest(res.clusterId()));

                mgr.send(headers, res.body());
            }
            catch (Throwable e) {
                log.error("Failed to send metrics to GMC", e);
            }
        }
    }

    /**
     * Pull metrics from cluster.
     */
    public void broadcastPullMetrics() {
        Collection<ClusterNode> nodes = ctx.grid().cluster().forServers().nodes();

        try {
            // TODO GG-22191 change on debug level.
            log.info("Broadcasting pull metrics request");

            MetricRequest req = new MetricRequest(-1);

            ctx.io().sendToGridTopic(nodes, TOPIC_METRICS, req, SYSTEM_POOL);
        }
        catch (Throwable e) {
            log.error("Failed to broadcast pull metrics request", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ctx.io().removeMessageListener(TOPIC_METRICS, lsnr);
    }
}
