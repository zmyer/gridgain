/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.selection.scoring.evaluator.metric.classification;

import org.apache.ignite.ml.selection.scoring.evaluator.aggregator.BinaryClassificationPointwiseMetricStatsAggregator;
import org.apache.ignite.ml.selection.scoring.evaluator.context.BinaryClassificationEvaluationContext;
import org.apache.ignite.ml.selection.scoring.evaluator.metric.Metric;

public abstract class BinaryClassificationMetric implements Metric<Double, BinaryClassificationEvaluationContext, BinaryClassificationPointwiseMetricStatsAggregator> {
    private final double truthLabel;
    private final double falseLabel;

    public BinaryClassificationMetric(double truthLabel, double falseLabel) {
        this.truthLabel = truthLabel;
        this.falseLabel = falseLabel;
    }

    public BinaryClassificationMetric() {
        truthLabel = Double.NaN;
        falseLabel = Double.NaN;
    }

    @Override public BinaryClassificationPointwiseMetricStatsAggregator makeAggregator() {
        if (Double.isNaN(truthLabel) && Double.isNaN(falseLabel))
            return new BinaryClassificationPointwiseMetricStatsAggregator();
        else
            return new BinaryClassificationPointwiseMetricStatsAggregator.WithCustomLabelsAggregator(truthLabel, falseLabel);
    }
}
