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

package org.apache.ignite.ml.nn.performance;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.nn.MLPTrainer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests {@link MLPTrainer} on the MNIST dataset that require to start the whole Ignite infrastructure.
 */
public class MLPTrainerMnistIntegrationTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /** Tests on the MNIST dataset. */
   /* @Test
    public void testMNIST() throws IOException {
        int featCnt = 28 * 28;
        int hiddenNeuronsCnt = 100;

        CacheConfiguration<Integer, MnistUtils.MnistLabeledImage> trainingSetCacheCfg = new CacheConfiguration<>();
        trainingSetCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 10));
        trainingSetCacheCfg.setName("MNIST_TRAINING_SET");
        IgniteCache<Integer, MnistUtils.MnistLabeledImage> trainingSet = ignite.createCache(trainingSetCacheCfg);

        int i = 0;
        for (MnistUtils.MnistLabeledImage e : MnistMLPTestUtil.loadTrainingSet(6_000))
            trainingSet.put(i++, e);

        MLPArchitecture arch = new MLPArchitecture(featCnt).
            withAddedLayer(hiddenNeuronsCnt, true, Activators.SIGMOID).
            withAddedLayer(10, false, Activators.SIGMOID);

        MLPTrainer<RPropParameterUpdate> trainer = new MLPTrainer<>(
            arch,
            LossFunctions.MSE,
            new UpdatesStrategy<>(
                new RPropUpdateCalculator(),
                RPropParameterUpdate.SUM,
                RPropParameterUpdate.AVG
            ),
            200,
            2000,
            10,
            123L
        );

        System.out.println("Start training...");
        long start = System.currentTimeMillis();
        MultilayerPerceptron mdl = trainer.fit(
            ignite,
            trainingSet,
            FeatureLabelExtractorWrapper.wrap(
                (k, v) -> VectorUtils.of(v.getPixels()),
                (k, v) -> VectorUtils.oneHot(v.getLabel(), 10).getStorage().data()
            )
        );
        System.out.println("Training completed in " + (System.currentTimeMillis() - start) + "ms");

        int correctAnswers = 0;
        int incorrectAnswers = 0;

        for (MnistUtils.MnistLabeledImage e : MnistMLPTestUtil.loadTestSet(1_000)) {
            Matrix input = new DenseMatrix(new double[][] {e.getPixels()});
            Matrix outputMatrix = mdl.predict(input);

            int predicted = (int)VectorUtils.vec2Num(outputMatrix.getRow(0));

            if (predicted == e.getLabel())
                correctAnswers++;
            else
                incorrectAnswers++;
        }

        double accuracy = 1.0 * correctAnswers / (correctAnswers + incorrectAnswers);
        assertTrue("Accuracy should be >= 80%", accuracy >= 0.8);
    }*/
}
