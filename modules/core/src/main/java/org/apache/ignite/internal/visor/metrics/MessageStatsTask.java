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
package org.apache.ignite.internal.visor.metrics;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.format;
import static org.apache.ignite.internal.IgniteFeatures.MESSAGE_PROFILING_AGGREGATION;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.SEPARATOR;
import static org.apache.ignite.internal.util.lang.GridFunc.transform;

public class MessageStatsTask extends VisorMultiNodeTask<MessageStatsTaskArg,
    MessageStatsTaskResult,
    MessageStatsTask.MessageStatsJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob job(MessageStatsTaskArg arg) {
        return new MessageStatsJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<MessageStatsTaskArg> arg) {
        IgnitePredicate<ClusterNode> pred = node ->
            nodeSupports(node.attribute(ATTR_IGNITE_FEATURES), MESSAGE_PROFILING_AGGREGATION)
            && (taskArg.nodeId() == null || taskArg.nodeId().equals(node.id()));

        return transform(ignite.cluster().forServers().forPredicate(pred).nodes(), ClusterNode::id);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected MessageStatsTaskResult reduce0(List<ComputeJobResult> results) throws IgniteException {
        if (taskArg.nodeId() != null)
            assert results.size() == 1;

        return reduceResults(results);
    }

    /** */
    private MessageStatsTaskResult reduceResults(List<ComputeJobResult> results) {
        Map<String, Map<Long, Long>> resMap = new HashMap<>();

        for (ComputeJobResult result : results) {
            MessageStatsJobResult jobResult = result.getData();

            jobResult.histograms().forEach((metricName, histogram) -> {
                Map<Long, Long> metricMap = resMap.computeIfAbsent(metricName, (name) -> new HashMap<>());

                long[] bounds =  histogram.bounds();
                long[] values = histogram.value();

                for (int i = 0; i < values.length; i++) {
                    long key = (i < values.length - 1) ? bounds[i] : Long.MAX_VALUE;

                    metricMap.put(key, metricMap.getOrDefault(key, 0L) + values[i]);
                }
            });
        }

        return new MessageStatsTaskResult(resMap);
    }

    /** */
    private long total(long[] values) {
        long res = 0;

        for (int i = 0; i < values.length; i++)
            res += values[i];

        return res;
    }

    /** */
    private Map<String, ? super Object> histogramToMap(HistogramMetric histogram) {
        Map<String, ? super Object> map = new HashMap<>();

        long[] bounds = histogram.bounds();
        long[] values = histogram.value();

        map.put("Total", total(values));

        for (int i = 0; i < bounds.length; i++)
            map.put("<= " + bounds[i], values[i]);

        map.put("> " + bounds[bounds.length - 1], values[values.length - 1]);

        return map;
    }

    /**
     *
     */
    public static class MessageStatsJob
        extends VisorJob<MessageStatsTaskArg, MessageStatsJobResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Ignite instance. */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected MessageStatsJob(@Nullable MessageStatsTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected MessageStatsJobResult run(@Nullable MessageStatsTaskArg arg)
            throws IgniteException {
            MetricRegistry registryHistograms = ignite.context().metric().registry(arg.metrics());

            if (registryHistograms == null)
                return getForMetric(arg.metrics());
            else
                return getForMetricGroup(arg.metrics(), registryHistograms);

            Map<String, HistogramMetric> histograms = new HashMap<>();
            Map<String, Long> time = new HashMap<>();

            if (findGrp) {
                String prefix = arg.metrics();

                for (Metric m : registryHistograms) {
                    if (m instanceof HistogramMetric && m.name().startsWith(prefix)) {
                        histograms.put(m.name(), (HistogramMetric)m);
                    }
                }
            }
            else {
                HistogramMetric m = registryHistograms.findMetric(arg.metrics());

                LongMetric total = registryHistograms.findMetric()

                assert m != null;

                histograms.put(m.name(), m);
            }

            return new MessageStatsJobResult(histograms, time);
        }

        private MessageStatsJobResult getForMetricGroup(String groupName, MetricRegistry registry) {

        }

        private MessageStatsJobResult getForMetric(String metricName) {
            Metric metric = ignite.context().metric().metricByFullName(metricName);

            if (!(metric instanceof HistogramMetric))
                throw new IgniteException("Can only collect statistics from histograms.");

            HistogramMetric histogram = (HistogramMetric)metric;

            histogram
        }

        private String histoMetricToTotal(String histogramName) {
            return "total" + new GridStringBuilder(histogramName.charAt(0)).a(histogramName.substring(1)).toString();
        }

        private IgnitePair<String> splitGroupAndName(String s) {
            int idx = s.lastIndexOf(SEPARATOR);

            if (idx <= 0 && idx > s.length() - 1)
                throw new IgniteException("Could not parse metric or registry name: " + s);

            String first = s.substring(0, idx - 1);
            String second = s.substring(idx + 1);

            return new IgnitePair<>(first, second);
        }
    }

    public static class MessageStatsJobResult extends VisorDataTransferObject {
        private Map<String, HistogramMetric> histograms;

        private Map<String, Long> time;

        public MessageStatsJobResult(
            Map<String, HistogramMetric> histograms, Map<String, Long> time) {
            this.histograms = histograms;
            this.time = time;
        }

        public Map<String, HistogramMetric> histograms() {
            return histograms;
        }

        public Map<String, Long> time() {
            return time;
        }

        /** {@inheritDoc} */
        @Override protected void writeExternalData(ObjectOutput out) throws IOException {
            U.writeMap(out, histograms);
            U.writeMap(out, time);
        }

        /** {@inheritDoc} */
        @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
            histograms = U.readMap(in);
            time = U.readMap(in);
        }
    }
}
