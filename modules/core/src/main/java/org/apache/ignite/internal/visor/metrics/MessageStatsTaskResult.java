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
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

public class MessageStatsTaskResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    private Map<String, HistogramDataHolder> histograms;

    private Map<String, Long> time;

    public MessageStatsTaskResult(String name, HistogramDataHolder histogram, Long time) {
        this(
            new HashMap<String, HistogramDataHolder>() {{ put(name, histogram); }},
            new HashMap<String, Long>() {{ put(name, time); }}
        );
    }

    public MessageStatsTaskResult(Map<String, HistogramDataHolder> histograms, Map<String, Long> time) {
        this.histograms = histograms;
        this.time = time;
    }

    public Map<String, HistogramDataHolder> histograms() {
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

    public static class HistogramDataHolder {
        private final long[] bounds;

        private final long[] values;

        public HistogramDataHolder(HistogramMetric histogram) {
            this(histogram.bounds(), histogram.value());
        }

        public HistogramDataHolder(long[] bounds, long[] values) {
            this.bounds = bounds;
            this.values = values;
        }

        public long[] bounds() {
            return bounds;
        }

        public long[] values() {
            return values;
        }
    }
}
