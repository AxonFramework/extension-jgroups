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
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.CommandResultMessage;
import org.axonframework.commandhandling.GenericCommandMessage;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.callbacks.FutureCallback;
import org.axonframework.commandhandling.distributed.AnnotationRoutingStrategy;
import org.axonframework.commandhandling.distributed.CommandBusConnectorCommunicationException;
import org.axonframework.commandhandling.distributed.DistributedCommandBus;
import org.axonframework.commandhandling.distributed.RoutingStrategy;
import org.axonframework.commandhandling.distributed.SimpleMember;
import org.axonframework.commandhandling.distributed.commandfilter.CommandNameFilter;
import org.axonframework.commandhandling.distributed.commandfilter.DenyAll;
import org.axonframework.extensions.jgroups.commandhandling.utils.MockException;
import org.axonframework.extensions.jgroups.commandhandling.utils.TestSerializer;
import org.axonframework.lifecycle.ShutdownInProgressException;
import org.axonframework.messaging.GenericMessage;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.serialization.SerializationException;
import org.axonframework.serialization.Serializer;
import org.axonframework.tracing.TestSpanFactory;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.stack.IpAddress;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.axonframework.commandhandling.distributed.SimpleMember.LOCAL_MEMBER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link JGroupsConnector}.
 *
 * @author Allard Buijze
 * @author Nakul Mishra
 */
class JGroupsConnectorTest {

    private JChannel channel1;
    private JChannel channel2;
    private JGroupsConnector connector1;
    private CommandBus mockCommandBus1;
    private DistributedCommandBus distributedCommandBus1;
    private JGroupsConnector connector2;
    private CommandBus mockCommandBus2;
    private DistributedCommandBus distributedCommandBus2;
    private String clusterName;
    private RoutingStrategy routingStrategy;
    private Serializer serializer = TestSerializer.xStreamSerializer();

    private TestSpanFactory testSpanFactory;

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

    @BeforeEach
    void setUp() throws Exception {
        routingStrategy = AnnotationRoutingStrategy.defaultStrategy();
        channel1 = createChannel();
        channel2 = createChannel();
        mockCommandBus1 = spy(SimpleCommandBus.builder().build());
        mockCommandBus2 = spy(SimpleCommandBus.builder().build());
        clusterName = "test-" + new Random().nextInt(Integer.MAX_VALUE);
        testSpanFactory = new TestSpanFactory();
        connector1 = JGroupsConnector.builder()
                                     .localSegment(mockCommandBus1)
                                     .channel(channel1)
                                     .clusterName(clusterName)
                                     .routingStrategy(routingStrategy)
                                     .serializer(serializer)
                                     .spanFactory(testSpanFactory)
                                     .build();
        connector2 = JGroupsConnector.builder()
                                     .localSegment(mockCommandBus2)
                                     .channel(channel2)
                                     .clusterName(clusterName)
                                     .routingStrategy(routingStrategy)
                                     .serializer(serializer)
                                     .spanFactory(testSpanFactory)
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

    @AfterEach
    void tearDown() {
        closeSilently(channel1);
        closeSilently(channel2);
    }

    @Test
    void setupOfReplyingCallback() throws Exception {
        final String mockPayload = "DummyString";
        final CommandMessage<String> commandMessage = new GenericCommandMessage<>(mockPayload);

        distributedCommandBus1.subscribe(String.class.getName(), m -> "ok");
        connector1.connect();
        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");

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
    @Test
    @Timeout(30000)
    void connectAndDispatchMessages_Balanced() throws Exception {
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

        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");
        assertTrue(connector2.awaitJoined(), "Connector 2 failed to connect");

        // wait for both connectors to have the same view
        waitForConnectorSync();

        List<FutureCallback<Object, Object>> callbacks = new ArrayList<>();

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
        verify(mockCommandBus1, atMost(40)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
        verify(mockCommandBus2, atLeast(60)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
        assertEquals(connector1.getConsistentHash(), connector2.getConsistentHash());
        assertNotNull(connector1.getNodeName());
        assertNotNull(connector2.getNodeName());
        assertNotEquals(connector1.getNodeName(), connector2.getNodeName());
    }

    @Test
    @Timeout(30000)
    void ringsProperlySynchronized_ChannelAlreadyConnectedToOtherCluster() throws Exception {
        channel1.connect("other");
        assertThrows(ConnectionFailedException.class, () -> connector1.connect());
    }

    @Test
    void messagesReceivedPromptlyAfterConnectingDoesntCauseException() throws Exception {
        channel2.connect(clusterName);
        channel1 = spy(channel1);
        connector1 = JGroupsConnector.builder()
                                     .localSegment(mockCommandBus1)
                                     .channel(channel1)
                                     .clusterName(clusterName)
                                     .serializer(serializer)
                                     .routingStrategy(routingStrategy)
                                     .build();

        doAnswer(i -> {
            connector1.receive(new ObjectMessage(
                    null,
                    new JoinMessage(100, new CommandNameFilter("test"), 0, false)).setSrc(channel2.address())
            );
            return i.callRealMethod();
        }).when(channel1).connect(any());
        connector1.connect();

        verify(channel1).connect(any());
    }

    @Test
    @Timeout(30000)
    void ringsProperlySynchronized_ChannelAlreadyConnected() throws Exception {
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);

        distributedCommandBus1.subscribe(String.class.getName(), new CountingCommandHandler(counter1));
        distributedCommandBus1.updateLoadFactor(20);
        connector1.connect();
        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");

        distributedCommandBus2.subscribe(Long.class.getName(), new CountingCommandHandler(counter2));
        distributedCommandBus2.updateLoadFactor(20);
        connector2.connect();

        assertTrue(connector2.awaitJoined(10, TimeUnit.SECONDS), "Connector 2 failed to connect");

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
    void joinMessageReceivedForDisconnectedHost() throws Exception {
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);

        distributedCommandBus1.subscribe(String.class.getName(), new CountingCommandHandler(counter1));
        distributedCommandBus1.updateLoadFactor(20);
        connector1.connect();

        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");

        distributedCommandBus2.subscribe(String.class.getName(), new CountingCommandHandler(counter2));
        distributedCommandBus2.updateLoadFactor(80);
        connector2.connect();
        assertTrue(connector2.awaitJoined(), "Connector 2 failed to connect");

        // wait for both connectors to have the same view
        waitForConnectorSync();

        // secretly insert an illegal message
        Message message = new ObjectMessage(channel1.getAddress(),
                                      new JoinMessage(10, DenyAll.INSTANCE, 0, true));
        message.setSrc(new IpAddress(12345));
        channel1.getReceiver().receive(message);

        assertFalse(
                connector1.getConsistentHash().getMembers().stream()
                          .map(i -> i.getConnectionEndpoint(Address.class).orElse(null))
                          .filter(Objects::nonNull)
                          .anyMatch(a -> a.equals(new IpAddress(12345))),
                "That message should not have changed the ring"
        );
    }

    @Test
    @Timeout(30000)
    void updatesToMemberShipProcessedInOrder() throws Exception {
        assertNull(connector1.getNodeName());
        assertNull(connector2.getNodeName());

        distributedCommandBus1.subscribe(String.class.getName(), new CountingCommandHandler(new AtomicInteger(0)));
        distributedCommandBus2.subscribe(String.class.getName(), new CountingCommandHandler(new AtomicInteger(0)));

        connector1.connect();
        connector2.connect();

        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");
        assertTrue(connector2.awaitJoined(), "Connector 2 failed to connect");

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
    void connectAndDispatchMessages_SingleCandidate() throws Exception {
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);

        distributedCommandBus1.subscribe(String.class.getName(), new CountingCommandHandler(counter1));
        distributedCommandBus1.updateLoadFactor(20);
        connector1.connect();
        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");

        distributedCommandBus2.subscribe(Object.class.getName(), new CountingCommandHandler(counter2));
        distributedCommandBus2.updateLoadFactor(80);
        connector2.connect();
        assertTrue(connector2.awaitJoined(), "Connector 2 failed to connect");

        // wait for both connectors to have the same view
        waitForConnectorSync();

        List<FutureCallback<Object, Object>> callbacks = new ArrayList<>();

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
        verify(mockCommandBus1, times(100)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
        verify(mockCommandBus2, never()).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
    }

    @Test
    void disconnectInvokesCallbacks() throws Throwable {
        BlockingCommandHandler handler1 = new BlockingCommandHandler();
        BlockingCommandHandler handler2 = new BlockingCommandHandler();

        distributedCommandBus1.subscribe(String.class.getName(), handler1);
        distributedCommandBus1.updateLoadFactor(1);
        connector1.connect();
        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");

        distributedCommandBus2.subscribe(String.class.getName(), handler2);
        distributedCommandBus2.updateLoadFactor(0);
        connector2.connect();
        assertTrue(connector2.awaitJoined(), "Connector 2 failed to connect");

        // wait for both connectors to have the same view
        waitForConnectorSync();

        FutureCallback<Object, Object> callback = new FutureCallback<>();

        distributedCommandBus2.dispatch(new GenericCommandMessage<>("message"), callback);
        handler1.awaitStarted();

        connector1.disconnect();

        CommandResultMessage<?> commandResultMessage = callback.get(10, TimeUnit.SECONDS);
        assertTrue(commandResultMessage.isExceptional());
        assertTrue(commandResultMessage.exceptionResult() instanceof CommandBusConnectorCommunicationException);

        handler1.finish();
        handler2.finish();
    }

    @Test
    void unserializableResponseReportedAsExceptional() throws Exception {
        serializer = spy(serializer);
        Object successResponse = new Object();
        Exception failureResponse = new MockException("This cannot be serialized");
        when(serializer.serialize(successResponse, byte[].class)).thenThrow(new SerializationException(
                "cannot serialize success"));

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
    void connectAndDispatchMessages_CustomCommandName() throws Exception {
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);

        distributedCommandBus1.subscribe("myCommand1", new CountingCommandHandler(counter1));
        distributedCommandBus1.updateLoadFactor(80);
        connector1.connect();
        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");

        distributedCommandBus2.subscribe("myCommand2", new CountingCommandHandler(counter2));
        distributedCommandBus2.updateLoadFactor(20);
        connector2.connect();
        assertTrue(connector2.awaitJoined(), "Connector 2 failed to connect");

        // wait for both connectors to have the same view
        waitForConnectorSync();

        List<FutureCallback<Object, Object>> callbacks = new ArrayList<>();

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
        verify(mockCommandBus1, times(34)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
        verify(mockCommandBus2, times(66)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
    }

    @Test
    void spanIsSetAndClosedWhenThereIsNoCallback() throws Exception {
        final AtomicInteger counter = new AtomicInteger(0);

        distributedCommandBus1.subscribe("myCommand1", new CountingCommandHandler(counter));
        connector1.connect();
        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");

        GenericCommandMessage<String> command = new GenericCommandMessage<>(new GenericMessage<>("message"),
                                                                            "myCommand1");
        connector1.send(connector1.findDestination(command).get(), command);

        await().untilAsserted(() -> assertEquals(1, counter.get()));
        testSpanFactory.verifySpanCompleted("JGroupsConnector.processDispatchMessage");
    }

    private void waitForConnectorSync() {
        await().pollDelay(Duration.ofMillis(50))
               .atMost(Duration.ofMillis(6000))
               .until(() -> connector1.getConsistentHash().equals(connector2.getConsistentHash()));
    }

    @Test
    void disconnectClosesJChannelConnection() throws Exception {
        connector1.connect();
        connector1.awaitJoined();

        connector1.disconnect();

        assertFalse(channel1.isConnected(), "Expected channel to be disconnected on connector.disconnect()");
    }

    @SuppressWarnings("unchecked")
    @Test
    void connectAndDispatchMessagesWaitingOnCallback() throws Exception {
        int numberOfDispatchedAndWaitedForCommands = 100;
        String firstCommandHandlerName = "firstCommandHandlerName";
        String secondCommandHandlerName = "secondCommandHandlerName";
        int commandHandlingCounter = 0;

        CommandDispatchingCommandHandler commandHandlerOne = new CommandDispatchingCommandHandler(
                numberOfDispatchedAndWaitedForCommands, distributedCommandBus1, secondCommandHandlerName
        );
        distributedCommandBus1.subscribe(firstCommandHandlerName, commandHandlerOne);
        connector1.connect();
        assertTrue(connector1.awaitJoined(10, TimeUnit.SECONDS), "Expected connector 1 to connect within 10 seconds");

        CommandDispatchingCommandHandler commandHandlerTwo = new CommandDispatchingCommandHandler(
                numberOfDispatchedAndWaitedForCommands, distributedCommandBus2, firstCommandHandlerName
        );
        distributedCommandBus2.subscribe(secondCommandHandlerName, commandHandlerTwo);
        connector2.connect();
        assertTrue(connector2.awaitJoined(), "Connector 2 failed to connect");

        // Wait for both connectors to have the same view
        waitForConnectorSync();

        FutureCallback<Integer, Integer> futureCallback = new FutureCallback<>();
        distributedCommandBus1.dispatch(
                new GenericCommandMessage<>(new GenericMessage<>(commandHandlingCounter), secondCommandHandlerName),
                futureCallback
        );

        assertEquals(numberOfDispatchedAndWaitedForCommands, futureCallback.getResult().getPayload().intValue());
        verify(mockCommandBus1, times(50)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
        verify(mockCommandBus2, times(50)).dispatch(any(CommandMessage.class), isA(CommandCallback.class));
    }

    @Test
    void localSegmentReturnsExpectedCommandBus() {
        Optional<CommandBus> result = connector1.localSegment();
        assertTrue(result.isPresent());
        assertEquals(mockCommandBus1, result.get());
    }

    @Test
    void stopSendingCommands() throws Exception {
        connector1.connect();
        connector1.awaitJoined(10, TimeUnit.SECONDS);

        Address localAddress = channel1.getAddress();
        String localName = localAddress.toString();
        SimpleMember<Address> me = new SimpleMember<>(localName, localAddress, LOCAL_MEMBER, null);

        //noinspection resource
        connector1.subscribe(String.class.getName(), message -> {
            Thread.sleep(200);
            return "great success";
        });

        AtomicReference<String> result = new AtomicReference<>();
        CommandMessage<String> command = GenericCommandMessage.asCommandMessage("command");
        connector1.send(me,
                        command,
                        (cm, crm) -> result.set((String) crm.getPayload()));
        connector1.initiateShutdown().get(400, TimeUnit.MILLISECONDS);
        assertEquals("great success", result.get());
        try {
            connector1.send(me, command);
            fail("After stopping no new commands should be accepted.");
        } catch (ShutdownInProgressException e) {
            // expected
        }
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

    private static class BlockingCommandHandler implements MessageHandler<CommandMessage<?>> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final CountDownLatch startedLatch = new CountDownLatch(1);

        @Override
        public Object handle(CommandMessage<?> message) {
            startedLatch.countDown();
            try {
                //noinspection ResultOfMethodCallIgnored
                finishLatch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {
                // test will fail
            }
            return null;
        }

        private void awaitStarted() throws InterruptedException {
            //noinspection ResultOfMethodCallIgnored
            startedLatch.await(30, TimeUnit.SECONDS);
        }

        private void finish() {
            finishLatch.countDown();
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
