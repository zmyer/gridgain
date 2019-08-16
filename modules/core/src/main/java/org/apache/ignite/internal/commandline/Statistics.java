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

import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.LongStream;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.visor.metrics.MessageStatsTask;
import org.apache.ignite.internal.visor.metrics.MessageStatsTaskArg;
import org.apache.ignite.internal.visor.metrics.MessageStatsTaskResult;

import static java.lang.String.format;
import static org.apache.ignite.internal.commandline.CommandList.STATISTICS;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.MetricsCommandArg.NODE;
import static org.apache.ignite.internal.commandline.MetricsCommandArg.STATS;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.argument.CommandArgUtils.parseArgs;

/**
 *
 */
public class Statistics implements Command<MessageStatsTaskArg> {
    private MessageStatsTaskArg arg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            MessageStatsTaskResult res = executeTask(client, MessageStatsTask.class, arg, clientCfg);

            printReport(arg.metrics(), res, logger);
        }

        return null;
    }

    private void printReport(String taskName, MessageStatsTaskResult taskResult, Logger log) {
        if (taskResult.histograms().isEmpty()) {
            log.info("No data for given metrics was found.");

            return;
        }

        String fmt1 = "%40s%12s%16s%14s%10s%10s%10s%10s%10s%10s%10s%10s%10s\n";
        String fmt2 = "%40s%12d%16f%14s%10s%10s%10s%10s%10s%10s%10s%10s%10s\n";

        log.info(format(fmt1, "", "", "", taskName, "", "", "", "", "", "", "", "", "", ""));

        MessageStatsTaskResult.HistogramDataHolder sampleHistogram = taskResult.histograms().values().iterator().next();

        Object[] captionFmtObjects = new Object[sampleHistogram.bounds().length + 4];

        captionFmtObjects[0] = "Message";
        captionFmtObjects[1] = "Total";
        captionFmtObjects[2] = "Total time(ms)";
        captionFmtObjects[3] = "Average(ms)";

        final int f = 4;

        for (int i = 0; i < sampleHistogram.bounds().length; i++)
            captionFmtObjects[i + f] = "<= " + sampleHistogram.bounds()[i];

        log.info(format(fmt1, captionFmtObjects));

        for (Map.Entry<String, MessageStatsTaskResult.HistogramDataHolder> entry : taskResult.histograms().entrySet()) {
            MessageStatsTaskResult.HistogramDataHolder histogram = entry.getValue();

            Object[] objects = new Object[4 + histogram.values().length];

            Long totalCount = LongStream.of(histogram.values()).sum();

            long totalTime = taskResult.time().getOrDefault(entry.getKey(), 0L);

            objects[0] = entry.getKey();
            objects[1] = totalCount;
            objects[2] = totalTime;
            objects[3] = totalTime == 0 ? 0 : totalCount.doubleValue() / totalTime;

            final int n = 4;

            for (int i = 0; i < histogram.values().length; i++)
                objects[i + n] = histogram.values()[i];

            log.info(format(fmt2, objects));
        }
    }

    /** {@inheritDoc} */
    @Override public MessageStatsTaskArg arg() {
        return arg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Prints requested node or cluster metrics or statistics.", STATISTICS, getOptions());
    }

    /** */
    private String[] getOptions() {
        return new String[] {
            STATS.toString(), "METRIC_GROUP_NAME",
            optional(NODE, "NODE_ID")
        };
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIterator) {
        Map<MetricsCommandArg, Class> argTypes = new EnumMap<MetricsCommandArg, Class>(MetricsCommandArg.class) {{
            put(NODE, UUID.class);
            put(STATS, String.class);
        }};

        Map<MetricsCommandArg, Object> parsedArgs = parseArgs(argIterator, MetricsCommandArg.class, argTypes);

        arg = new MessageStatsTaskArg(
            (UUID) parsedArgs.get(NODE),
            (String) parsedArgs.get(STATS)
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return STATISTICS.toCommandName();
    }
}
