/*
 * Copyright (c) 2010-2023. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.jgroups.commandhandling;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandExecutionException;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.CommandResultMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.callbacks.FutureCallback;
import org.axonframework.commandhandling.distributed.AnnotationRoutingStrategy;
import org.axonframework.commandhandling.distributed.DistributedCommandBus;
import org.axonframework.commandhandling.distributed.RoutingStrategy;
import org.axonframework.commandhandling.distributed.commandfilter.AcceptAll;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.extensions.jgroups.commandhandling.utils.TestSerializer;
import org.axonframework.messaging.GenericMessage;
import org.axonframework.messaging.HandlerExecutionException;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.RemoteHandlingException;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.jgroups.JChannel;
import org.jgroups.stack.GossipRouter;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link JGroupsConnector} with a {@link GossipRouter} in between.
 *
 * @author Allard Buijze
 * @author Nakul Mishra
 */
class JGroupsConnectorGossipTest {

    private JGroupsConnector connector1;
    private JGroupsConnector connector2;
    private GossipRouter gossipRouter;
    private Serializer serializer;

    private static JChannel createChannel() throws Exception {
        return new JChannel("org/axonframework/extensions/jgroups/commandhandling/tcp_gossip.xml");
    }

    @BeforeEach
    void setUp() throws Exception {
        JChannel channel1 = createChannel();
        JChannel channel2 = createChannel();
        RoutingStrategy routingStrategy = AnnotationRoutingStrategy.defaultStrategy();
        CommandBus mockCommandBus1 = spy(SimpleCommandBus.builder().build());
        CommandBus mockCommandBus2 = spy(SimpleCommandBus.builder().build());
        String clusterName = "test-" + new Random().nextInt(Integer.MAX_VALUE);
        serializer = spy(TestSerializer.xStreamSerializer());
        connector1 = JGroupsConnector.builder()
                                     .localSegment(mockCommandBus1)
                                     .channel(channel1)
                                     .clusterName(clusterName)
                                     .serializer(serializer)
                                     .routingStrategy(routingStrategy)
                                     .build();
        connector2 = JGroupsConnector.builder()
                                     .localSegment(mockCommandBus2)
                                     .channel(channel2)
                                     .clusterName(clusterName)
                                     .serializer(serializer)
                                     .routingStrategy(routingStrategy)
                                     .build();
        gossipRouter = new GossipRouter("127.0.0.1", 12001);
    }

    @AfterEach
    void tearDown() {
        if (gossipRouter != null) {
            gossipRouter.stop();
        }
    }

    @Test
    @Timeout(75000)
    void connectorRecoversWhenGossipRouterReconnects() throws Exception {

        connector1.updateMembership(20, AcceptAll.INSTANCE);
        connector1.connect();
        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");

        connector2.updateMembership(80, AcceptAll.INSTANCE);
        connector2.connect();

        assertTrue(connector2.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 2 to connect within 10 seconds");

        // the nodes joined, but can't detect each other yet
        gossipRouter.start();

        // now, they should detect each other and start syncing their state
        await().pollDelay(Duration.ofMillis(100))
               .atMost(Duration.ofMillis(60000))
               .until(() -> connector1.getConsistentHash().equals(connector2.getConsistentHash()));
    }

    @Test
    @Timeout(30000)
    void customResponseIncludedInReply() throws Exception {
        gossipRouter.start();

        DistributedCommandBus bus1 = DistributedCommandBus.builder()
                                                          .commandRouter(connector1)
                                                          .connector(connector1)
                                                          .build();
        DistributedCommandBus bus2 = DistributedCommandBus.builder()
                                                          .commandRouter(connector2)
                                                          .connector(connector2)
                                                          .build();

        bus2.subscribe("test", m -> {
            throw new CommandExecutionException("Generic message", new RuntimeException(), m.getPayload());
        });
        connector2.connect();
        connector1.connect();
        waitForConnectorSync();

        FutureCallback<String, Object> callback = new FutureCallback<>();
        bus1.dispatch(new GenericCommandMessage<>(new GenericMessage<>("hello world"), "test"), callback);

        CommandResultMessage<?> actual = callback.join();
        assertTrue(actual.isExceptional());
        assertTrue(actual.exceptionResult() instanceof HandlerExecutionException);
        assertEquals("hello world", actual.exceptionDetails().orElse(null));
    }

    @Test
    @Timeout(30000)
    void distributedCommandBusInvokesCallbackOnSerializationFailure() throws Exception {
        gossipRouter.start();

        final AtomicInteger counter2 = new AtomicInteger(0);

        DistributedCommandBus bus1 = DistributedCommandBus.builder()
                                                          .commandRouter(connector1)
                                                          .connector(connector1)
                                                          .build();
        bus1.updateLoadFactor(20);
        connector1.connect();
        assertTrue(connector1.awaitJoined(5, TimeUnit.SECONDS), "Failed to connect");

        DistributedCommandBus bus2 = DistributedCommandBus.builder()
                                                          .commandRouter(connector2)
                                                          .connector(connector2)
                                                          .build();
        bus2.subscribe(String.class.getName(), new CountingCommandHandler(counter2));
        bus2.updateLoadFactor(20);
        connector2.connect();
        assertTrue(connector2.awaitJoined(5, TimeUnit.SECONDS), "Failed to connect");

        // now, they should detect each other and start syncing their state
        waitForConnectorSync();

        CommandGateway gateway1 = DefaultCommandGateway.builder().commandBus(bus1).build();

        doThrow(new RuntimeException("Mock"))
                .when(serializer)
                .deserialize(argThat((ArgumentMatcher<SerializedObject<byte[]>>) x -> Arrays.equals(
                        "<string>Try this!</string>".getBytes(StandardCharsets.UTF_8), x.getData()
                )));

        try {
            gateway1.sendAndWait("Try this!");
            fail("Expected exception");
        } catch (CommandExecutionException e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof RemoteHandlingException);
            RemoteHandlingException remoteHandlingException = (RemoteHandlingException) cause;
            assertTrue(remoteHandlingException.getExceptionDescriptions().stream().anyMatch(m -> m.contains("Mock")),
                       "Wrong exception. \nConsistent hash status of connector2: \n" + connector2.getConsistentHash());
        }
    }

    private void waitForConnectorSync() {
        await().pollDelay(Duration.ofMillis(100))
               .atMost(Duration.ofMillis(20000))
               .until(() -> connector1.getConsistentHash().equals(connector2.getConsistentHash()));
    }

    private static class CountingCommandHandler implements MessageHandler<CommandMessage<?>> {

        private final AtomicInteger counter;

        private CountingCommandHandler(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public Object handle(CommandMessage<?> message) {
            counter.incrementAndGet();
            return "The Reply!";
        }
    }
}
