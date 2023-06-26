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

package org.axonframework.extensions.jgroups.autoconfig;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.commandhandling.distributed.AnnotationRoutingStrategy;
import org.axonframework.commandhandling.distributed.CommandBusConnector;
import org.axonframework.commandhandling.distributed.CommandRouter;
import org.axonframework.commandhandling.distributed.DistributedCommandBus;
import org.axonframework.commandhandling.distributed.RoutingStrategy;
import org.axonframework.commandhandling.gateway.AbstractCommandGateway;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.eventhandling.EventBus;
import org.axonframework.extensions.jgroups.commandhandling.JGroupsConnector;
import org.axonframework.serialization.Serializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the {@link JGroupsAutoConfiguration}.
 *
 * @author Allard Buijze
 */
@ExtendWith(SpringExtension.class)
class JGroupsAutoConfigurationTest {

    private ApplicationContextRunner testApplicationContext;

    @BeforeEach
    void setUp() {
        testApplicationContext = new ApplicationContextRunner()
                .withUserConfiguration(TestContext.class)
                .withPropertyValues("axon.distributed.enabled:true",
                                    "axon.distributed.jgroups.gossip.auto-start:true",
                                    "axon.distributed.jgroups.bind-addr:127.0.0.1");
    }

    @Test
    void contextInitialization() {
        testApplicationContext.run(context -> {
            RoutingStrategy routingStrategy = context.getBean(RoutingStrategy.class);
            assertNotNull(routingStrategy);
            assertEquals(AnnotationRoutingStrategy.class, routingStrategy.getClass());

            CommandRouter commandRouter = context.getBean(CommandRouter.class);
            assertNotNull(commandRouter);
            assertEquals(JGroupsConnector.class, commandRouter.getClass());

            CommandBusConnector commandBusConnector = context.getBean(CommandBusConnector.class);
            assertNotNull(commandBusConnector);
            assertEquals(JGroupsConnector.class, commandBusConnector.getClass());

            assertSame(commandRouter, commandBusConnector);

            CommandBus commandBus = context.getBean("commandBus", CommandBus.class);
            assertNotNull(commandBus);
            assertEquals(SimpleCommandBus.class, commandBus.getClass());

            CommandBus distributedCommandBus = context.getBean("distributedCommandBus", CommandBus.class);
            assertNotNull(distributedCommandBus);
            assertEquals(DistributedCommandBus.class, distributedCommandBus.getClass());

            assertNotSame(distributedCommandBus, commandBus);

            assertNotNull(context.getBean(EventBus.class));
            CommandGateway gateway = context.getBean(CommandGateway.class);
            assertTrue(gateway instanceof DefaultCommandGateway);
            assertSame(((AbstractCommandGateway) gateway).getCommandBus(), distributedCommandBus);
            assertNotNull(gateway);
            assertNotNull(context.getBean(Serializer.class));
        });
    }

    @ContextConfiguration
    @EnableAutoConfiguration(exclude = {
            JmxAutoConfiguration.class,
            WebClientAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class,
            DataSourceAutoConfiguration.class
    })
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    public static class TestContext {

    }
}
