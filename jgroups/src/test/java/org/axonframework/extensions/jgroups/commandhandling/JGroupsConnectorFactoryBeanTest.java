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
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.distributed.RoutingStrategy;
import org.axonframework.extensions.jgroups.commandhandling.utils.TestSerializer;
import org.axonframework.serialization.Serializer;
import org.jgroups.JChannel;
import org.junit.jupiter.api.*;
import org.springframework.context.ApplicationContext;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link JGroupsConnectorFactoryBean}.
 *
 * @author Allard Buijze
 */
class JGroupsConnectorFactoryBeanTest {

    private ConnectorInstantiationExposingFactoryBean testSubject;

    private ApplicationContext mockApplicationContext;
    private JChannel mockChannel;
    private JGroupsConnector mockConnector;
    private final Serializer serializer = TestSerializer.xStreamSerializer();

    @BeforeEach
    void setUp() {
        mockApplicationContext = mock(ApplicationContext.class);
        mockChannel = mock(JChannel.class);
        mockConnector = mock(JGroupsConnector.class);
        when(mockApplicationContext.getBean(Serializer.class)).thenReturn(serializer);
        testSubject = spy(new ConnectorInstantiationExposingFactoryBean());
        testSubject.setChannelFactory(() -> mockChannel);
        testSubject.setBeanName("beanName");
        testSubject.setApplicationContext(mockApplicationContext);
    }

    @Test
    void createWithDefaultValues() throws Exception {
        testSubject.afterPropertiesSet();
        testSubject.start();
        //noinspection ResultOfMethodCallIgnored
        testSubject.getObject();

        verify(testSubject).instantiateConnector(
                isA(SimpleCommandBus.class),
                eq(mockChannel),
                eq("beanName"),
                isA(Serializer.class),
                isA(RoutingStrategy.class));
        verify(mockConnector).connect();
        verify(mockChannel, never()).close();

        testSubject.stop(() -> {
        });

        verify(mockChannel).close();
    }

    @Test
    void createWithSpecifiedValues() throws Exception {
        testSubject.setClusterName("ClusterName");
        testSubject.setSerializer(serializer);
        SimpleCommandBus localSegment = SimpleCommandBus.builder().build();
        testSubject.setLocalSegment(localSegment);
        RoutingStrategy routingStrategy = CommandMessage::getCommandName;
        testSubject.setRoutingStrategy(routingStrategy);
        testSubject.setChannelName("localName");
        testSubject.afterPropertiesSet();
        testSubject.start();
        //noinspection ResultOfMethodCallIgnored
        testSubject.getObject();

        verify(testSubject).instantiateConnector(
                same(localSegment),
                eq(mockChannel),
                eq("ClusterName"),
                same(serializer),
                same(routingStrategy));
        verify(mockApplicationContext, never()).getBean(Serializer.class);
        verify(mockChannel).setName("localName");
        verify(mockConnector).connect();
        verify(mockChannel, never()).close();

        testSubject.stop(() -> {
        });

        verify(mockChannel).close();
    }

    @Test
    void createWithCustomConfigurationFile() {
        testSubject.setConfiguration("does-not-exist");
        testSubject.setClusterName("ClusterName");
        testSubject.setSerializer(serializer);
        SimpleCommandBus localSegment = SimpleCommandBus.builder().build();
        testSubject.setLocalSegment(localSegment);
        RoutingStrategy routingStrategy = CommandMessage::getCommandName;
        testSubject.setRoutingStrategy(routingStrategy);
        testSubject.setChannelName("localName");
        try {
            testSubject.afterPropertiesSet();
            fail("Expected a failed attempt to load inexistent settings");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("does-not-exist"));
        }
    }

    @Test
    void simpleProperties() {
        assertEquals(Integer.MAX_VALUE, testSubject.getPhase());
        testSubject.setPhase(100);
        assertEquals(100, testSubject.getPhase());
        assertTrue(testSubject.isAutoStartup());
    }

    private class ConnectorInstantiationExposingFactoryBean extends JGroupsConnectorFactoryBean {

        @Override
        public JGroupsConnector instantiateConnector(CommandBus localSegment, JChannel channel, String clusterName,
                                                     Serializer serializer, RoutingStrategy routingStrategy) {
            return mockConnector;
        }
    }
}
