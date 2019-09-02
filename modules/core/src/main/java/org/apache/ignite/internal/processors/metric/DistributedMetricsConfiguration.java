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
package org.apache.ignite.internal.processors.metric;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;

import static java.lang.String.format;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MESSAGE_STATS_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STAT_TOO_LONG_PROCESSING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_STAT_TOO_LONG_WAITING;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedBooleanProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty.detachedLongProperty;

public class DistributedMetricsConfiguration {
    private static final boolean DEFAULT_DIAGNOSTIC_MESSAGE_STATS_ENABLED = getBoolean(IGNITE_MESSAGE_STATS_ENABLED, true);

    private static final long DEFAULT_DIAGNOSTIC_MESSAGE_STATS_TOO_LONG_PROCESSING =
        TimeUnit.MILLISECONDS.toNanos(getLong(IGNITE_STAT_TOO_LONG_PROCESSING, 250));

    private static final long DEFAULT_DIAGNOSTIC_MESSAGE_STATS_TOO_LONG_WAITING =
        TimeUnit.MILLISECONDS.toNanos(getLong(IGNITE_STAT_TOO_LONG_WAITING, 250));

    private final IgniteLogger log;

    private final DistributedBooleanProperty diagnosticMessageStatsEnabled = detachedBooleanProperty("diagnosticMessageStatsEnabled");

    private final DistributedLongProperty diagnosticMessageStatTooLongProcessing = detachedLongProperty("diagnosticMessageStatTooLongProcessing");

    private final DistributedLongProperty diagnosticMessageStatTooLongWaiting = detachedLongProperty("diagnosticMessageStatTooLongWaiting");

    public DistributedMetricsConfiguration(GridInternalSubscriptionProcessor subscriptionProcessor, IgniteLogger log) {
        this.log = log;

        subscriptionProcessor.registerDistributedConfigurationListener(dispatcher -> {
            diagnosticMessageStatsEnabled.addListener(this::updateListener);
            diagnosticMessageStatTooLongProcessing.addListener(this::updateListener);
            diagnosticMessageStatTooLongWaiting.addListener(this::updateListener);

            dispatcher.registerProperty(diagnosticMessageStatsEnabled);
            dispatcher.registerProperty(diagnosticMessageStatTooLongProcessing);
            dispatcher.registerProperty(diagnosticMessageStatTooLongWaiting);
        });
    }

    private <T> void updateListener(String key, T oldVal, T newVal) {
        log.info(format("Metric distributed property '%s' was changed, oldVal: '%s', newVal: '%s'", key, oldVal, newVal));
    }

    public boolean diagnosticMessageStatsEnabled() {
        return diagnosticMessageStatsEnabled.getOrDefault(DEFAULT_DIAGNOSTIC_MESSAGE_STATS_ENABLED);
    }

    public void diagnosticMessageStatsEnabled(boolean diagnosticMessageStatsEnabled) {
        try {
            this.diagnosticMessageStatsEnabled.propagateAsync(diagnosticMessageStatsEnabled);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    public long diagnosticMessageStatTooLongProcessing() {
        return TimeUnit.NANOSECONDS.toMillis(
            diagnosticMessageStatTooLongProcessing.getOrDefault(DEFAULT_DIAGNOSTIC_MESSAGE_STATS_TOO_LONG_PROCESSING)
        );
    }

    public void diagnosticMessageStatTooLongProcessing(long diagnosticMessageStatTooLongProcessing) {
        try {
            this.diagnosticMessageStatTooLongProcessing
                .propagate(TimeUnit.MILLISECONDS.toNanos(diagnosticMessageStatTooLongProcessing));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    public long diagnosticMessageStatTooLongWaiting() {
        return TimeUnit.NANOSECONDS.toMillis(
            diagnosticMessageStatTooLongWaiting.getOrDefault(DEFAULT_DIAGNOSTIC_MESSAGE_STATS_TOO_LONG_WAITING)
        );
    }

    public void diagnosticMessageStatTooLongWaiting(long diagnosticMessageStatTooLongWaiting) {
        try {
            this.diagnosticMessageStatTooLongWaiting
                .propagateAsync(TimeUnit.MILLISECONDS.toNanos(diagnosticMessageStatTooLongWaiting));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
