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

import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.LongStream;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.argument.CommandParameter;
import org.apache.ignite.internal.commandline.argument.CommandParameterConfig;
import org.apache.ignite.internal.visor.statistics.MessageStatsTask;
import org.apache.ignite.internal.visor.statistics.MessageStatsTaskArg;
import org.apache.ignite.internal.visor.statistics.MessageStatsTaskResult;

import static java.lang.String.format;
import static org.apache.ignite.internal.commandline.CommandList.STATISTICS;
import static org.apache.ignite.internal.commandline.StatisticsCommandArg.NODE;
import static org.apache.ignite.internal.commandline.StatisticsCommandArg.STATS;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.argument.CommandArgUtils.parseArgs;

/**
 *
 */
public class Statistics implements Command<MessageStatsTaskArg> {
    /** */
    private final CommandParameterConfig<StatisticsCommandArg> STATS_PARAMS = new CommandParameterConfig<>(
        new CommandParameter(NODE, UUID.class, true),
        new CommandParameter(STATS, MessageStatsTaskArg.StatisticsType.class)
    );


    /** */
    private MessageStatsTaskArg arg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            MessageStatsTaskResult res = executeTask(client, MessageStatsTask.class, arg, clientCfg);

            printReport(arg.statisticsType().toString(), res, logger);
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

        Object[] captionFmtObjects = new Object[taskResult.bounds().length + 4];

        captionFmtObjects[0] = "Message";
        captionFmtObjects[1] = "Total";
        captionFmtObjects[2] = "Total time(ms)";
        captionFmtObjects[3] = "Average(ms)";

        final int f = 4;

        for (int i = 0; i < taskResult.bounds().length; i++)
            captionFmtObjects[i + f] = "<= " + taskResult.bounds()[i];

        log.info(format(fmt1, captionFmtObjects));

        taskResult.histograms().forEach((metric, values) -> {
            Object[] objects = new Object[4 + values.length];

            Long totalCount = LongStream.of(values).sum();

            long totalTime = taskResult.totalMetric().getOrDefault(metric, 0L);

            objects[0] = metric;
            objects[1] = totalCount;
            objects[2] = totalTime;
            objects[3] = totalTime == 0 ? 0 : totalCount.doubleValue() / totalTime;

            final int n = 4;

            for (int i = 0; i < values.length; i++)
                objects[i + n] = values[i];

            log.info(format(fmt2, objects));
        });
    }

    /** {@inheritDoc} */
    @Override public MessageStatsTaskArg arg() {
        return arg;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Command.usage(logger, "Prints requested node or cluster metrics or statistics.", STATISTICS, STATS_PARAMS.optionsUsage());
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIterator) {
        Map<StatisticsCommandArg, Object> parsedArgs = parseArgs(
            argIterator,
            StatisticsCommandArg.class,
            STATS_PARAMS
        );

        arg = new MessageStatsTaskArg(
            (UUID) parsedArgs.get(NODE),
            (MessageStatsTaskArg.StatisticsType)parsedArgs.get(STATS)
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return STATISTICS.toCommandName();
    }
}
