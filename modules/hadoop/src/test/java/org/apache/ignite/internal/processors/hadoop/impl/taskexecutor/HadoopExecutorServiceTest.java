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

package org.apache.ignite.internal.processors.hadoop.impl.taskexecutor;

import org.apache.ignite.internal.processors.hadoop.taskexecutor.HadoopExecutorService;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAdder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class HadoopExecutorServiceTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecutesAll() throws Exception {
        final HadoopExecutorService exec = new HadoopExecutorService(log, "_GRID_NAME_", 10, 5);

        for (int i = 0; i < 5; i++) {
            final int loops = 5000;
            int threads = 17;

            final LongAdder sum = new LongAdder();

            multithreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < loops; i++) {
                        exec.submit(new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                sum.increment();

                                return null;
                            }
                        });
                    }

                    return null;
                }
            }, threads);

            while (exec.active() != 0) {
                X.println("__ active: " + exec.active());

                Thread.sleep(200);
            }

            assertEquals(threads * loops, sum.sum());

            X.println("_ ok");
        }

        assertTrue(exec.shutdown(0));
    }
}