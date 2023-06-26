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
import org.axonframework.commandhandling.distributed.CommandBusConnector;
import org.axonframework.commandhandling.distributed.CommandRouter;
import org.axonframework.commandhandling.distributed.ConsistentHashChangeListener;
import org.axonframework.commandhandling.distributed.DistributedCommandBus;
import org.axonframework.commandhandling.distributed.RoutingStrategy;
import org.axonframework.extensions.jgroups.DistributedCommandBusProperties;
import org.axonframework.extensions.jgroups.commandhandling.JGroupsConnectorFactoryBean;
import org.axonframework.serialization.Serializer;
import org.axonframework.springboot.autoconfig.InfraConfiguration;
import org.axonframework.tracing.SpanFactory;
import org.jgroups.stack.GossipRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AutoConfiguration
@AutoConfigureAfter(RoutingStrategyAutoConfiguration.class)
@AutoConfigureBefore(InfraConfiguration.class)
@EnableConfigurationProperties(DistributedCommandBusProperties.class)
@ConditionalOnProperty("axon.distributed.enabled")
@ConditionalOnClass(name = {
        "org.axonframework.extensions.jgroups.commandhandling.JGroupsConnector",
        "org.jgroups.JChannel"
})
public class JGroupsAutoConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(JGroupsAutoConfiguration.class);

    @Autowired
    private DistributedCommandBusProperties properties;

    @ConditionalOnProperty("axon.distributed.jgroups.gossip.autoStart")
    @Bean(destroyMethod = "stop")
    public GossipRouter gossipRouter() {
        Matcher matcher =
                Pattern.compile("([^[\\[]]*)\\[(\\d*)\\]").matcher(properties.getJgroups().getGossip().getHosts());
        if (matcher.find()) {

            GossipRouter gossipRouter = new GossipRouter(matcher.group(1), Integer.parseInt(matcher.group(2)));
            try {
                gossipRouter.start();
            } catch (Exception e) {
                logger.warn("Unable to autostart start embedded Gossip server: {}", e.getMessage());
            }
            return gossipRouter;
        } else {
            logger.error("Wrong hosts pattern, cannot start embedded Gossip Router: " +
                                 properties.getJgroups().getGossip().getHosts());
        }
        return null;
    }

    @ConditionalOnMissingBean({CommandRouter.class, CommandBusConnector.class})
    @Bean
    public JGroupsConnectorFactoryBean jgroupsConnectorFactoryBean(
            @Qualifier("messageSerializer") Serializer messageSerializer,
            @Qualifier("localSegment") CommandBus localSegment,
            RoutingStrategy routingStrategy,
            @Autowired(required = false) ConsistentHashChangeListener consistentHashChangeListener,
            SpanFactory spanFactory
    ) {
        System.setProperty("jgroups.tunnel.gossip_router_hosts", properties.getJgroups().getGossip().getHosts());
        System.setProperty("jgroups.bind_addr", String.valueOf(properties.getJgroups().getBindAddr()));
        System.setProperty("jgroups.bind_port", String.valueOf(properties.getJgroups().getBindPort()));

        JGroupsConnectorFactoryBean jGroupsConnectorFactoryBean = new JGroupsConnectorFactoryBean();
        jGroupsConnectorFactoryBean.setClusterName(properties.getJgroups().getClusterName());
        jGroupsConnectorFactoryBean.setLocalSegment(localSegment);
        jGroupsConnectorFactoryBean.setSerializer(messageSerializer);
        jGroupsConnectorFactoryBean.setConfiguration(properties.getJgroups().getConfigurationFile());
        if (consistentHashChangeListener != null) {
            jGroupsConnectorFactoryBean.setConsistentHashChangeListener(consistentHashChangeListener);
        }
        jGroupsConnectorFactoryBean.setRoutingStrategy(routingStrategy);
        jGroupsConnectorFactoryBean.setSpanFactory(spanFactory);
        return jGroupsConnectorFactoryBean;
    }

    @Bean
    @Primary
    @ConditionalOnBean(CommandBusConnector.class)
    @ConditionalOnMissingBean
    public DistributedCommandBus distributedCommandBus(
            CommandRouter commandRouter,
            CommandBusConnector commandBusConnector,
            DistributedCommandBusProperties distributedCommandBusProperties,
            SpanFactory spanFactory
    ) {
        DistributedCommandBus commandBus = DistributedCommandBus.builder()
                                                                .commandRouter(commandRouter)
                                                                .connector(commandBusConnector)
                                                                .spanFactory(spanFactory)
                                                                .build();
        commandBus.updateLoadFactor(distributedCommandBusProperties.getLoadFactor());
        return commandBus;
    }
}
