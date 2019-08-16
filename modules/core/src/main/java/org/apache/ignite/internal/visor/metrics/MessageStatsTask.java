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
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.SEPARATOR;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.splitRegistryAndMetricName;
import static org.apache.ignite.internal.util.lang.GridFunc.transform;

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
        if (taskArg.nodeId() != null)
            assert results.size() == 1;

        return reduceResults(results);
    }

    /** */
    private MessageStatsTaskResult reduceResults(List<ComputeJobResult> results) {
        Map<String, MessageStatsTaskResult.HistogramDataHolder> reducedHistograms = new HashMap<>();
        Map<String, Long> reducedTotalTimeMetrics = new HashMap<>();

        for (ComputeJobResult result : results) {
            MessageStatsTaskResult jobResult = result.getData();

            jobResult.histograms().forEach((metricName, histogram) -> {
                addToReducedHistogram(reducedHistograms, metricName, histogram);

                long newVal = reducedTotalTimeMetrics.getOrDefault(metricName, 0L) + jobResult.time().get(metricName);

                reducedTotalTimeMetrics.put(metricName, newVal);
            });
        }

        return new MessageStatsTaskResult(reducedHistograms, reducedTotalTimeMetrics);
    }

    private void addToReducedHistogram(
        Map<String, MessageStatsTaskResult.HistogramDataHolder> reducedMap,
        String name,
        MessageStatsTaskResult.HistogramDataHolder histogram
    ) {
        MessageStatsTaskResult.HistogramDataHolder reduced = reducedMap.computeIfAbsent(
            name,
            k -> new MessageStatsTaskResult.HistogramDataHolder(histogram.bounds(), histogram.values())
        );

        for (int i = 0; i < reduced.values().length; i++) {
            if (i >= histogram.values().length)
                //this should never happen
                throw new IgniteException("Received different histograms from nodes, can't reduce");

            reduced.values()[i] += histogram.values()[i];
        }
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

    static MessageStatsTaskResult.HistogramDataHolder histogramHolder(HistogramMetric metric) {
        return new MessageStatsTaskResult.HistogramDataHolder(metric);
    }

    /**
     *
     */
    public static class MessageStatsJob
        extends VisorJob<MessageStatsTaskArg, MessageStatsTaskResult> {
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
            MetricRegistry registryHistograms = ignite.context().metric().registry(arg.metrics());

            if (registryHistograms == null)
                return getForMetric(arg.metrics());
            else
                return getForMetricGroup(arg.metrics());
        }

        private MetricRegistry getRegistryWithCheck(String metricName, @Nullable String registryName) {
            if (registryName == null)
                throw new IgniteException("Could not find registry for metric: " + metricName);

            MetricRegistry registry = ignite.context().metric().registry(registryName);

            if (registry == null)
                throw new IgniteException("Could not find metric registry: " + registryName);

            return registry;
        }

        private <T> T getMetricWithCheck(MetricRegistry registry, String name, Class<T> cls) {
            Metric metric = registry.findMetric(name);

            if (metric == null)
                throw new IgniteException("No such metric '" + name + "' in registry: " + registry.name());

            return (T) metric;
        }

        private MessageStatsTaskResult getForMetricGroup(String groupName) {
            MetricRegistry registry = getRegistryWithCheck(groupName, groupName);

            Map<String, MessageStatsTaskResult.HistogramDataHolder> histogramMap = new HashMap<>();
            Map<String, Long> timeMap = new HashMap<>();

            IgnitePair<String> names = splitRegistryAndMetricName(groupName);

            MetricRegistry totalTimeRegistry;

            if (names.get1() != null) {
                String totalTimeRegistryName = names.get1() + SEPARATOR + histoMetricToTotal(names.get2());

                totalTimeRegistry = ignite.context().metric().registry(totalTimeRegistryName);
            }
            else
                totalTimeRegistry = null;

            registry.forEach(metric -> {
                if (!(metric instanceof HistogramMetric))
                    return;

                HistogramMetric histogram = (HistogramMetric)metric;

                histogramMap.put(histogram.name(), histogramHolder(histogram));

                long totalTime = 0;

                if (totalTimeRegistry != null) {
                    Metric totalTimeMetric = totalTimeRegistry.findMetric(metric.name());

                    if (totalTimeMetric != null && totalTimeMetric instanceof LongMetric)
                        totalTime = ((LongMetric)totalTimeMetric).value();
                }

                timeMap.put(metric.name(), totalTime);
            });

            return new MessageStatsTaskResult(histogramMap, timeMap);
        }

        private MessageStatsTaskResult getForMetric(String metricName) {
            IgnitePair<String> metricNames = splitRegistryAndMetricName(metricName);

            HistogramMetric histogram =
                getMetricWithCheck(getRegistryWithCheck(metricName, metricNames.get1()), metricNames.get2(), HistogramMetric.class);

            IgnitePair<String> names = splitRegistryAndMetricName(metricNames.get1());

            long totalTime = 0;

            if (names.get1() != null) {
                String totalTimeRegistryName = names.get1() + SEPARATOR + histoMetricToTotal(names.get2());

                MetricRegistry totalTimeRegistry = ignite.context().metric().registry(totalTimeRegistryName);

                if (totalTimeRegistry != null) {
                    Metric totalTimeMetric = totalTimeRegistry.findMetric(metricNames.get2());

                    if (totalTimeMetric != null && totalTimeMetric instanceof LongMetric)
                        totalTime = ((LongMetric)totalTimeMetric).value();
                }
            }

            return new MessageStatsTaskResult(metricNames.get2(), histogramHolder(histogram), totalTime);
        }

        private String histoMetricToTotal(String histogramName) {
            return "total" + new GridStringBuilder(Character.toUpperCase(histogramName.charAt(0)))
                .a(histogramName.substring(1)).toString();
        }
    }
}
