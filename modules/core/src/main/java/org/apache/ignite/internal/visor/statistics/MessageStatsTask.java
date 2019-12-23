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
package org.apache.ignite.internal.visor.statistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteFeatures.MESSAGE_PROFILING_AGGREGATION;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_FEATURES;
import static org.apache.ignite.internal.managers.communication.GridIoManager.DIAGNOSTICS_MESSAGES;
import static org.apache.ignite.internal.managers.communication.GridIoManager.MSG_STAT_PROCESSING_TIME;
import static org.apache.ignite.internal.managers.communication.GridIoManager.MSG_STAT_QUEUE_WAITING_TIME;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.DIAGNOSTIC_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.SEPARATOR;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.splitRegistryAndMetricName;
import static org.apache.ignite.internal.util.lang.GridFunc.transform;

/**
 *
 */
@GridInternal
public class MessageStatsTask extends VisorMultiNodeTask<MessageStatsTaskArg, MessageStatsTaskResult, MessageStatsTaskResult> {
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
        assert taskArg.nodeId() == null || results.size() == 1;

        return reduceResults(results);
    }

    /** */
    private MessageStatsTaskResult reduceResults(List<ComputeJobResult> results) {
        Map<String, Long> timeMap = new HashMap<>();
        Map<String, long[]> histogramsMap = new HashMap<>();

        AtomicReference<long[]> bounds = null;

        for (ComputeJobResult result : results) {
            MessageStatsTaskResult jobResult = result.getData();

            if (bounds.get() == null)
                bounds.set(jobResult.bounds());

            jobResult.histograms().forEach((metricName, histogram) -> {
                assert histogram.length == bounds.get().length + 1;

                addToReducedHistogram(histogramsMap, metricName, histogram);

                long newVal = timeMap.getOrDefault(metricName, 0L) + jobResult.totalMetric().get(metricName);

                timeMap.put(metricName, newVal);
            });
        }

        return new MessageStatsTaskResult(timeMap, bounds.get(), histogramsMap);
    }

    private void addToReducedHistogram(
        Map<String, long[]> reducedMap,
        String name,
        long[] histogramValues
    ) {
        long[] reduced = reducedMap.computeIfAbsent(name, k -> new long[histogramValues.length]);

        for (int i = 0; i < reduced.length; i++) {
            if (i >= histogramValues.length)
                //this should never happen
                throw new IgniteException("Received different histograms from nodes, can't reduce");

            reduced[i] += histogramValues[i];
        }
    }

    /**
     *
     */
    public static class MessageStatsJob extends VisorJob<MessageStatsTaskArg, MessageStatsTaskResult> {
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
        @Override protected MessageStatsTaskResult run(@Nullable MessageStatsTaskArg arg)
            throws IgniteException {
            MetricRegistry registryHistograms = histogramsRegistry(arg.statisticsType());

            MetricRegistry totalTimeRegistry = totalTimeRegistry(registryHistograms.name());

            Map<String, Long> timeMap = new HashMap<>();
            Map<String, long[]> histogramsMap = new HashMap<>();

            AtomicReference<long[]> bounds = new AtomicReference(null);

            registryHistograms.forEach(metric -> {
                IgnitePair<String> names = splitRegistryAndMetricName(metric.name());

                HistogramMetric histogram = (HistogramMetric)metric;

                histogramsMap.put(names.get2(), histogram.value());

                if (bounds.get() == null)
                    bounds.set(histogram.bounds());

                Metric totalTimeMetric = totalTimeRegistry.findMetric(metric.name());

                if (!(totalTimeMetric instanceof LongMetric))
                    throw new IgniteException("Total time metric was now found for '" + metric.name() + "'");

                timeMap.put(names.get2(), ((LongMetric)totalTimeMetric).value());

            });

            return new MessageStatsTaskResult(timeMap, bounds.get(), histogramsMap);
        }

        /** */
        private MetricRegistry histogramsRegistry(MessageStatsTaskArg.StatisticsType statsType) {
            return getRegistryWithCheck(registryName(statsType));
        }

        /** */
        private MetricRegistry totalTimeRegistry(String histogramsRegistryName) {
            IgnitePair<String> names = splitRegistryAndMetricName(histogramsRegistryName);

            String commonPart = names.get1();

            if (commonPart == null)
                commonPart = histogramsRegistryName;

            return getRegistryWithCheck(commonPart + SEPARATOR + histoMetricToTotal(names.get1()));
        }

        private String registryName(MessageStatsTaskArg.StatisticsType statsType) {
            switch (statsType) {
                case PROCESSING: return metricName(DIAGNOSTIC_METRICS, DIAGNOSTICS_MESSAGES, MSG_STAT_PROCESSING_TIME);
                case QUEUE_WAITING: return metricName(DIAGNOSTIC_METRICS, DIAGNOSTICS_MESSAGES, MSG_STAT_QUEUE_WAITING_TIME);
            }

            return null;
        }

        private MetricRegistry getRegistryWithCheck(@Nullable String registryName) {
            MetricRegistry registry = ignite.context().metric().registry(registryName);

            if (registry == null)
                throw new IgniteException("Could not find metric registry: " + registryName);

            return registry;
        }

        /** */
        private String histoMetricToTotal(String histogramName) {
            return "total" + new GridStringBuilder()
                .a(Character.toUpperCase(histogramName.charAt(0)))
                .a(histogramName.substring(1)).toString();
        }
    }
}
