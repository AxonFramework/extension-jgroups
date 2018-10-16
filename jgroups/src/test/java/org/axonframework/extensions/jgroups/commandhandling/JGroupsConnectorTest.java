/*
 * Copyright (c) 2010-2018. Axon Framework
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
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.CommandResultMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.callbacks.FutureCallback;
import org.axonframework.commandhandling.distributed.AnnotationRoutingStrategy;
import org.axonframework.commandhandling.distributed.DistributedCommandBus;
import org.axonframework.commandhandling.distributed.RoutingStrategy;
import org.axonframework.commandhandling.distributed.UnresolvedRoutingKeyPolicy;
import org.axonframework.commandhandling.distributed.commandfilter.DenyAll;
import org.axonframework.extensions.jgroups.commandhandling.utils.MockException;
import org.axonframework.messaging.GenericMessage;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.serialization.SerializationException;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.stack.IpAddress;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Allard Buijze
 * @author Nakul Mishra
 */
public class JGroupsConnectorTest {

    private JChannel channel1;
    private JChannel channel2;
    private JGroupsConnector connector1;
    private CommandBus mockCommandBus1;
    private DistributedCommandBus distributedCommandBus1;
    private JGroupsConnector connector2;
    private CommandBus mockCommandBus2;
    private DistributedCommandBus distributedCommandBus2;
    private Serializer serializer;
    private String clusterName;
    private RoutingStrategy routingStrategy;

    @Before
    public void setUp() throws Exception {
        routingStrategy = new AnnotationRoutingStrategy(UnresolvedRoutingKeyPolicy.RANDOM_KEY);
        channel1 = createChannel();
        channel2 = createChannel();
        mockCommandBus1 = spy(SimpleCommandBus.builder().build());
        mockCommandBus2 = spy(SimpleCommandBus.builder().build());
        clusterName = "test-" + new Random().nextInt(Integer.MAX_VALUE);
        serializer = XStreamSerializer.builder().build();
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

        distributedCommandBus1 = DistributedCommandBus.builder()
                                                      .commandRouter(connector1)
                                                      .connector(connector1)
                                                      .build();
        distributedCommandBus2 = DistributedCommandBus.builder()
                                                      .commandRouter(connector2)
                                                      .connector(connector2)
                                                      .build();
    }

    @After
    public void tearDown() {
        closeSilently(channel1);
        closeSilently(channel2);
    }

    @Test
    public void testSetupOfReplyingCallback() throws Exception {
        final String mockPayload = "DummyString";
        final CommandMessage<String> commandMessage = new GenericCommandMessage<>(mockPayload);

        distributedCommandBus1.subscribe(String.class.getName(), m -> "ok");
        connector1.connect();
        assertTrue("Expected connector 1 to connect within 10 seconds", connector1.awaitJoined(10, TimeUnit.SECONDS));

        connector1.awaitJoined();

        FutureCallback<String, Object> futureCallback = new FutureCallback<>();
        distributedCommandBus1.dispatch(commandMessage, futureCallback);
        futureCallback.awaitCompletion(10, TimeUnit.SECONDS);

        //Verify that the newly introduced ReplyingCallBack class is being wired in. Actual behaviour of
        // ReplyingCallback is tested in its unit tests
        //noinspection unchecked
        verify(mockCommandBus1).dispatch(argThat(x -> x != null && x.getPayload().equals(mockPayload)),
                                         any(CommandCallback.class));
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 30000)
    public void testConnectAndDispatchMessages_Balanced() throws Exception {
        assertNull(connector1.getNodeName());
        assertNull(connector2.getNodeName());

        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);

        distributedCommandBus1.subscribe(String.class.getName(), new CountingCommandHandler(counter1));
        distributedCommandBus2.subscribe(String.class.getName(), new CountingCommandHandler(counter2));

        distributedCommandBus1.updateLoadFactor(20);
        distributedCommandBus2.updateLoadFactor(80);

        connector1.connect();
        connector2.connect();

        assertTrue("Expected connector 1 to connect within 10 seconds", connector1.awaitJoined(10, TimeUnit.SECONDS));
        assertTrue("Connector 2 failed to connect", connector2.awaitJoined());

        // wait for both connectors to have the same view
        waitForConnectorSync();

        List<FutureCallback> callbacks = new ArrayList<>();

        //noinspection Duplicates
        for (int t = 0; t < 100; t++) {
            FutureCallback<Object, Object> callback = new FutureCallback<>();
            String message = "message" + t;
            if ((t & 1) == 0) {
                distributedCommandBus1.dispatch(new GenericCommandMessage<>(message), callback);
            } else {
                distributedCommandBus2.dispatch(new GenericCommandMessage<>(message), callback);
            }
            callbacks.add(callback);
        }
        for (FutureCallback<?, ?> callback : callbacks) {
            assertEquals("The Reply!", callback.get().getPayload());
        }
        assertEquals(100, counter1.get() + counter2.get());
        System.out.println("Node 1 got " + counter1.get());
        System.out.println("Node 2 got " + counter2.get());
        verify(mockCommandBus1, atMost(40)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
        verify(mockCommandBus2, atLeast(60)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
        assertEquals(connector1.getConsistentHash(), connector2.getConsistentHash());
        assertNotNull(connector1.getNodeName());
        assertNotNull(connector2.getNodeName());
        assertNotEquals(connector1.getNodeName(), connector2.getNodeName());
    }

    @Test(expected = ConnectionFailedException.class, timeout = 30000)
    public void testRingsProperlySynchronized_ChannelAlreadyConnectedToOtherCluster() throws Exception {
        channel1.connect("other");
        connector1.connect();
    }

    @Test(timeout = 30000)
    public void testRingsProperlySynchronized_ChannelAlreadyConnected() throws Exception {
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);

        distributedCommandBus1.subscribe(String.class.getName(), new CountingCommandHandler(counter1));
        distributedCommandBus1.updateLoadFactor(20);
        connector1.connect();
        assertTrue("Expected connector 1 to connect within 10 seconds", connector1.awaitJoined(10, TimeUnit.SECONDS));

        distributedCommandBus2.subscribe(Long.class.getName(), new CountingCommandHandler(counter2));
        distributedCommandBus2.updateLoadFactor(20);
        connector2.connect();

        assertTrue("Connector 2 failed to connect", connector2.awaitJoined(10, TimeUnit.SECONDS));

        waitForConnectorSync();

        FutureCallback<Object, Object> callback1 = new FutureCallback<>();
        distributedCommandBus1.dispatch(new GenericCommandMessage<>("Hello"), callback1);
        FutureCallback<Object, Object> callback2 = new FutureCallback<>();
        distributedCommandBus1.dispatch(new GenericCommandMessage<>(1L), callback2);

        FutureCallback<Object, Object> callback3 = new FutureCallback<>();
        distributedCommandBus2.dispatch(new GenericCommandMessage<>("Hello"), callback3);
        FutureCallback<Object, Object> callback4 = new FutureCallback<>();
        distributedCommandBus2.dispatch(new GenericCommandMessage<>(1L), callback4);

        assertEquals("The Reply!", callback1.get().getPayload());
        assertEquals("The Reply!", callback2.get().getPayload());
        assertEquals("The Reply!", callback3.get().getPayload());
        assertEquals("The Reply!", callback4.get().getPayload());

        assertEquals(connector1.getConsistentHash(), connector2.getConsistentHash());
    }

    @Test
    public void testJoinMessageReceivedForDisconnectedHost() throws Exception {
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);

        distributedCommandBus1.subscribe(String.class.getName(), new CountingCommandHandler(counter1));
        distributedCommandBus1.updateLoadFactor(20);
        connector1.connect();

        assertTrue("Expected connector 1 to connect within 10 seconds", connector1.awaitJoined(10, TimeUnit.SECONDS));

        distributedCommandBus2.subscribe(String.class.getName(), new CountingCommandHandler(counter2));
        distributedCommandBus2.updateLoadFactor(80);
        connector2.connect();
        assertTrue("Connector 2 failed to connect", connector2.awaitJoined());

        // wait for both connectors to have the same view
        waitForConnectorSync();

        // secretly insert an illegal message
        Message message = new Message(channel1.getAddress(),
                                      new JoinMessage(10, DenyAll.INSTANCE, 0, true));
        message.setSrc(new IpAddress(12345));
        channel1.getReceiver().receive(message);

        assertFalse("That message should not have changed the ring",
                    connector1.getConsistentHash().getMembers().stream()
                              .map(i -> i.getConnectionEndpoint(Address.class).orElse(null))
                              .filter(Objects::nonNull)
                              .anyMatch(a -> a.equals(new IpAddress(12345))));
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 30000)
    public void testUpdatesToMemberShipProcessedInOrder() throws Exception {
        assertNull(connector1.getNodeName());
        assertNull(connector2.getNodeName());

        distributedCommandBus1.subscribe(String.class.getName(), new CountingCommandHandler(new AtomicInteger(0)));
        distributedCommandBus2.subscribe(String.class.getName(), new CountingCommandHandler(new AtomicInteger(0)));

        connector1.connect();
        connector2.connect();

        assertTrue("Expected connector 1 to connect within 10 seconds", connector1.awaitJoined(10, TimeUnit.SECONDS));
        assertTrue("Connector 2 failed to connect", connector2.awaitJoined());

        // wait for both connectors to have the same view
        waitForConnectorSync();

        for (int i = 0; i <= 100; i = i + 10) {
            distributedCommandBus1.updateLoadFactor(i);
        }
        // send some fake news
        channel1.send(null, new JoinMessage(1, DenyAll.INSTANCE, 0, false));

        waitForConnectorSync();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConnectAndDispatchMessages_SingleCandidate() throws Exception {
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);

        distributedCommandBus1.subscribe(String.class.getName(), new CountingCommandHandler(counter1));
        distributedCommandBus1.updateLoadFactor(20);
        connector1.connect();
        assertTrue("Expected connector 1 to connect within 10 seconds", connector1.awaitJoined(10, TimeUnit.SECONDS));

        distributedCommandBus2.subscribe(Object.class.getName(), new CountingCommandHandler(counter2));
        distributedCommandBus2.updateLoadFactor(80);
        connector2.connect();
        assertTrue("Connector 2 failed to connect", connector2.awaitJoined());

        // wait for both connectors to have the same view
        waitForConnectorSync();

        List<FutureCallback> callbacks = new ArrayList<>();

        //noinspection Duplicates
        for (int t = 0; t < 100; t++) {
            FutureCallback<Object, Object> callback = new FutureCallback<>();
            String message = "message" + t;
            if ((t & 1) == 0) {
                distributedCommandBus1.dispatch(new GenericCommandMessage<>(message), callback);
            } else {
                distributedCommandBus2.dispatch(new GenericCommandMessage<>(message), callback);
            }
            callbacks.add(callback);
        }
        for (FutureCallback<?, ?> callback : callbacks) {
            assertEquals("The Reply!", callback.get().getPayload());
        }
        assertEquals(100, counter1.get() + counter2.get());
        System.out.println("Node 1 got " + counter1.get());
        System.out.println("Node 2 got " + counter2.get());
        verify(mockCommandBus1, times(100)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
        verify(mockCommandBus2, never()).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
    }

    @Test
    public void testUnserializableResponseReportedAsExceptional() throws Exception {
        serializer = spy(XStreamSerializer.builder().build());
        Object successResponse = new Object();
        Exception failureResponse = new MockException("This cannot be serialized");
        when(serializer.serialize(successResponse, byte[].class)).thenThrow(new SerializationException(
                "cannot serialize success"));
        when(serializer.serialize(failureResponse, byte[].class)).thenThrow(new SerializationException(
                "cannot serialize failure"));

        connector1 = JGroupsConnector.builder()
                                     .localSegment(mockCommandBus1)
                                     .channel(channel1)
                                     .clusterName(clusterName)
                                     .serializer(serializer)
                                     .routingStrategy(routingStrategy)
                                     .build();
        distributedCommandBus1 = DistributedCommandBus.builder()
                                                      .commandRouter(connector1)
                                                      .connector(connector1)
                                                      .build();

        distributedCommandBus1.subscribe(String.class.getName(), c -> successResponse);
        distributedCommandBus1.subscribe(Integer.class.getName(), c -> {
            throw failureResponse;
        });
        connector1.connect();

        FutureCallback<Object, Object> callback = new FutureCallback<>();

        distributedCommandBus1.dispatch(new GenericCommandMessage<>(1), callback);
        CommandResultMessage<?> result = callback.getResult();
        assertTrue(result.isExceptional());
        //noinspection unchecked
        verify(mockCommandBus1).dispatch(any(CommandMessage.class), isA(CommandCallback.class));

        callback = new FutureCallback<>();
        distributedCommandBus1.dispatch(new GenericCommandMessage<>("string"), callback);
        assertTrue(callback.getResult().isExceptional());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConnectAndDispatchMessages_CustomCommandName() throws Exception {
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);

        distributedCommandBus1.subscribe("myCommand1", new CountingCommandHandler(counter1));
        distributedCommandBus1.updateLoadFactor(80);
        connector1.connect();
        assertTrue("Expected connector 1 to connect within 10 seconds", connector1.awaitJoined(10, TimeUnit.SECONDS));

        distributedCommandBus2.subscribe("myCommand2", new CountingCommandHandler(counter2));
        distributedCommandBus2.updateLoadFactor(20);
        connector2.connect();
        assertTrue("Connector 2 failed to connect", connector2.awaitJoined());

        // wait for both connectors to have the same view
        waitForConnectorSync();

        List<FutureCallback> callbacks = new ArrayList<>();

        for (int t = 0; t < 100; t++) {
            FutureCallback<Object, Object> callback = new FutureCallback<>();
            String message = "message" + t;
            if ((t % 3) == 0) {
                distributedCommandBus1.dispatch(
                        new GenericCommandMessage<>(new GenericMessage<>(message), "myCommand1"),
                        callback);
            } else {
                distributedCommandBus2.dispatch(
                        new GenericCommandMessage<>(new GenericMessage<>(message), "myCommand2"),
                        callback);
            }
            callbacks.add(callback);
        }
        for (FutureCallback<?, ?> callback : callbacks) {
            assertEquals("The Reply!", callback.get().getPayload());
        }
        assertEquals(100, counter1.get() + counter2.get());
        System.out.println("Node 1 got " + counter1.get());
        System.out.println("Node 2 got " + counter2.get());
        verify(mockCommandBus1, times(34)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
        verify(mockCommandBus2, times(66)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
    }

    private void waitForConnectorSync() throws InterruptedException {
        int t = 0;
        while (!connector1.getConsistentHash().equals(connector2.getConsistentHash())) {
            // don't have a member for String yet, which means we must wait a little longer
            if (t++ > 300) {
                assertEquals("Connectors did not synchronize within 15 seconds.", connector1.getConsistentHash(),
                             connector2.getConsistentHash());
            }
            Thread.sleep(50);
        }
        Thread.yield();
    }

    @Test
    public void testDisconnectClosesJChannelConnection() throws Exception {
        connector1.connect();
        connector1.awaitJoined();

        connector1.disconnect();

        assertFalse("Expected channel to be disconnected on connector.disconnect()", channel1.isConnected());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConnectAndDispatchMessagesWaitingOnCallback() throws Exception {
        Integer numberOfDispatchedAndWaitedForCommands = 100;
        String firstCommandHandlerName = "firstCommandHandlerName";
        String secondCommandHandlerName = "secondCommandHandlerName";
        int commandHandlingCounter = 0;

        CommandDispatchingCommandHandler commandHandlerOne = new CommandDispatchingCommandHandler(
                numberOfDispatchedAndWaitedForCommands, distributedCommandBus1, secondCommandHandlerName
        );
        distributedCommandBus1.subscribe(firstCommandHandlerName, commandHandlerOne);
        connector1.connect();
        assertTrue("Expected connector 1 to connect within 10 seconds", connector1.awaitJoined(10, TimeUnit.SECONDS));

        CommandDispatchingCommandHandler commandHandlerTwo = new CommandDispatchingCommandHandler(
                numberOfDispatchedAndWaitedForCommands, distributedCommandBus2, firstCommandHandlerName
        );
        distributedCommandBus2.subscribe(secondCommandHandlerName, commandHandlerTwo);
        connector2.connect();
        assertTrue("Connector 2 failed to connect", connector2.awaitJoined());

        // Wait for both connectors to have the same view
        waitForConnectorSync();

        FutureCallback<Integer, Integer> futureCallback = new FutureCallback<>();
        distributedCommandBus1.dispatch(
                new GenericCommandMessage<>(new GenericMessage<>(commandHandlingCounter), secondCommandHandlerName),
                futureCallback
        );

        assertEquals(numberOfDispatchedAndWaitedForCommands, futureCallback.getResult().getPayload());
        verify(mockCommandBus1, times(50)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
        verify(mockCommandBus2, times(50)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
    }

    private static void closeSilently(JChannel channel) {
        try {
            channel.close();
        } catch (Exception e) {
            // ignore
        }
    }

    private static JChannel createChannel() throws Exception {
        return new JChannel("org/axonframework/extensions/jgroups/commandhandling/tcp_static.xml");
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

    private static class CommandDispatchingCommandHandler implements MessageHandler<CommandMessage<?>> {

        private final int counterMax;
        private final CommandBus commandBus;
        private final String commandHandlerName;

        private CommandDispatchingCommandHandler(int counterMax, CommandBus commandBus, String commandHandlerName) {
            this.counterMax = counterMax;
            this.commandBus = commandBus;
            this.commandHandlerName = commandHandlerName;
        }

        @Override
        public Integer handle(CommandMessage<?> commandMessage) {
            Integer commandHandlingCounter = (Integer) commandMessage.getPayload();
            int count = ++commandHandlingCounter;
            if (count == counterMax) {
                return counterMax;
            }

            FutureCallback<Object, Object> futureCallback = new FutureCallback<>();
            commandBus.dispatch(
                    new GenericCommandMessage<>(new GenericMessage<>(commandHandlingCounter), commandHandlerName),
                    futureCallback
            );
            return (Integer) futureCallback.getResult(1000, TimeUnit.MILLISECONDS).getPayload();
        }
    }
}
