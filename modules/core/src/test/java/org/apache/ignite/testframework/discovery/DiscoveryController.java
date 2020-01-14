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
package org.apache.ignite.testframework.discovery;

import java.util.Iterator;
import java.util.Queue;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.hamcrest.Matcher;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertTrue;

/**
 * Ignite wrapper for providing a convenient way to manage the discovery messages flow.
 */
public class DiscoveryController {
    /** Ignite representation. */
    private final IgniteEx ignite;

    /** Test discovery SPI for managing messages. */
    private final BlockedDiscoverySpi spi;

    /** Holder of processed events. */
    private final Queue<DiscoveryEvent> processedEvents;// = new ConcurrentLinkedQueue<>();

    /**
     * @param ignite Ignite with {@link BlockedDiscoverySpi}.
     */
    public DiscoveryController(IgniteEx ignite, Queue<DiscoveryEvent> queue) {
        this.ignite = ignite;

        processedEvents = queue;
//        ignite.context().event().addLocalEventListener(
//            event -> processedEvents.add((DiscoveryEvent)event),
//            EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED, EVT_DISCOVERY_CUSTOM_EVT
//        );

        if (!(ignite.configuration().getDiscoverySpi() instanceof BlockedDiscoverySpi))
            throw new RuntimeException("BlockedDiscoverySpi should be configured");

        this.spi = (BlockedDiscoverySpi)ignite.configuration().getDiscoverySpi();
    }

    /**
     * Helper method to await the start of processing a message selected by the condition.
     *
     * @param predicate Condition for awaiting.
     * @throws IgniteInterruptedCheckedException If fail.
     */
    public void awaitProcessingMessage(
        Matcher<TcpDiscoveryAbstractMessage> predicate) throws IgniteInterruptedCheckedException {
        assertTrue(
            waitForCondition(() -> {
                    Iterator<TcpDiscoveryAbstractMessage> it = startProcessingMessages().iterator();
                    while (it.hasNext())
                        if (predicate.matches(it.next()))
                            return true;

                    return false;
                },
                10000)
        );
    }

    /**
     * Helper method to await the start of processing a message selected by the condition.
     *
     * @param predicate Condition for awaiting.
     * @throws IgniteInterruptedCheckedException If fail.
     */
    public void awaitProcessedEvent(Matcher<DiscoveryEvent> predicate) throws IgniteInterruptedCheckedException {
        assertTrue(
            waitForCondition(() -> {
                    Iterator<DiscoveryEvent> it = processedEvents().iterator();
                    while (it.hasNext())
                        if (predicate.matches(it.next()))
                            return true;

                    return false;
                },
                10000)
        );
    }

    /**
     * Helper method to await the start of processing a message selected by the condition.
     *
     * @param predicate Condition for awaiting.
     * @throws IgniteInterruptedCheckedException If fail.
     */
    public void awaitReceivedMessage(
        Matcher<TcpDiscoveryAbstractMessage> predicate) throws IgniteInterruptedCheckedException {
        assertTrue(
            waitForCondition(() -> {
                    Iterator<TcpDiscoveryAbstractMessage> it = receivedMessages().iterator();
                    while (it.hasNext())
                        if (predicate.matches(it.next()))
                            return true;

                    return false;
                },
                10000)
        );
    }

    /**
     * Register condition by which execution of message would be blocked.
     *
     * @param predicate Condition for blocking.
     * @throws IgniteInterruptedCheckedException If fail.
     */
    public void blockIf(Matcher<TcpDiscoveryAbstractMessage> predicate) throws IgniteInterruptedCheckedException {
        spi.blockIf(predicate);
    }

    /**
     * Awaiting the event of blocking. It will fail if blocking doesn't happen for some time.
     *
     * @throws IgniteInterruptedCheckedException If fail.
     */
    public void awaitBlocking() throws IgniteInterruptedCheckedException {
        spi.awaitBlocking();
    }

    /**
     * Awaiting the event of blocking then release this for further execution.
     *
     * @throws IgniteInterruptedCheckedException If fail.
     */
    public void releaseWhenBlocked() throws IgniteInterruptedCheckedException {
        awaitBlocking();

        release();
    }

    /**
     * Trigger to continue to handle the message.
     */
    public void release() {
        spi.release();
    }

    /**
     * Message execution will be failed if it is blocked by the registered condition.
     */
    public void failWhenBlocked() throws IgniteInterruptedCheckedException {
        awaitBlocking();

        spi.failIfBlocked();
    }

    /**
     * @param msg Custom message to send.
     * @param <T> Certain type of custom message.
     * @throws IgniteCheckedException If fail.
     */
    public <T extends DiscoveryCustomMessage> void sendCustomEvent(T msg) throws IgniteCheckedException {
        ignite.context().discovery().sendCustomEvent(msg);
    }

    /**
     * @return Events which was processed.
     */
    public Iterable<DiscoveryEvent> processedEvents() {
        return processedEvents;
    }

    /**
     * @return All messages processing of which was started. Processing means only started of message handle but not a
     * call of listeners.
     */
    public Iterable<TcpDiscoveryAbstractMessage> startProcessingMessages() {
        return spi.startProcessingMessages();
    }

    /**
     * @return All messages which would send to next node.
     */
    public Iterable<TcpDiscoveryAbstractMessage> sentMessages() {
        return spi.sentMessages();
    }

    /**
     * @return All messages which were read from the socket from the previous node.
     */
    public Iterable<TcpDiscoveryAbstractMessage> receivedMessages() {
        return spi.receivedMessages();
    }

    /** */
    public UUID localNodeId() {
        return ignite.localNode().id();
    }
}
