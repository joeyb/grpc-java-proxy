package org.joeyb.grpc.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.ServerCall;

import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class HostHeaderProxyChannelManagerTests {

    @Test
    public void constructorThrowsIfLocalChannelBuilderConfigurerIsNull() {
        assertThatThrownBy(() -> new HostHeaderProxyChannelManager(null, UUID.randomUUID().toString(), (h, c) -> { }))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("localChannelBuilderConfigurer");
    }

    @Test
    public void constructorThrowsIfLocalHostNameIsNull() {
        assertThatThrownBy(() -> new HostHeaderProxyChannelManager(c -> { }, null, (h, c) -> { }))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("localHostName");
    }

    @Test
    public void constructorThrowsIfRemoteChannelBuilderConfigurerIsNull() {
        assertThatThrownBy(() -> new HostHeaderProxyChannelManager(c -> { }, UUID.randomUUID().toString(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("remoteChannelBuilderConfigurer");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void constructorSetsFieldsCorrectly() {
        Consumer<ManagedChannelBuilder<?>> localChannelBuilderConfigurer = mock(Consumer.class);
        String localHostName = UUID.randomUUID().toString();
        BiConsumer<String, ManagedChannelBuilder<?>> remoteChannelBuilderConfigurer = mock(BiConsumer.class);

        HostHeaderProxyChannelManager proxyChannelManager = new HostHeaderProxyChannelManager(
                localChannelBuilderConfigurer,
                localHostName,
                remoteChannelBuilderConfigurer);

        assertThat(proxyChannelManager.channels)
                .isNotNull()
                .isEmpty();

        assertThat(proxyChannelManager.localChannelBuilderConfigurer).isSameAs(localChannelBuilderConfigurer);
        assertThat(proxyChannelManager.localHostName).isEqualTo(localHostName);
        assertThat(proxyChannelManager.remoteChannelBuilderConfigurer).isSameAs(remoteChannelBuilderConfigurer);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getChannelReturnsLocalChannelWhenGivenNoHostHeader() {
        AtomicBoolean isLocalChannelBuilderConfigured = new AtomicBoolean();
        String localHostName = UUID.randomUUID().toString();

        Consumer<ManagedChannelBuilder<?>> localChannelBuilderConfigurer =
                c -> {
                    if (!isLocalChannelBuilderConfigured.compareAndSet(false, true)) {
                        fail("localChannelBuilderConfigurer should only be executed once.");
                    }
                };

        BiConsumer<String, ManagedChannelBuilder<?>> remoteChannelBuilderConfigurer =
                (h, c) -> fail("remoteChannelBuilderConfigurer should not be executed.");

        ManagedChannelBuilder<?> channelBuilder = mock(ManagedChannelBuilder.class);
        ManagedChannel channel = mock(ManagedChannel.class);

        when(channelBuilder.build()).thenReturn(channel);

        HostHeaderProxyChannelManager proxyChannelManager = new HostHeaderProxyChannelManager(
                localChannelBuilderConfigurer,
                localHostName,
                remoteChannelBuilderConfigurer) {

            @Override
            protected ManagedChannelBuilder<?> createLocalChannelBuilder(String host) {
                assertThat(host).isEqualTo(localHostName);

                return channelBuilder;
            }
        };

        Channel actualChannel = proxyChannelManager.getChannel(mock(ServerCall.class), new Metadata());

        assertThat(isLocalChannelBuilderConfigured.get()).isTrue();
        assertThat(actualChannel).isSameAs(channel);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getChannelReturnsLocalChannelWhenGivenLocalHostHeader() {
        AtomicBoolean isLocalChannelBuilderConfigured = new AtomicBoolean();
        String localHostName = UUID.randomUUID().toString();

        Consumer<ManagedChannelBuilder<?>> localChannelBuilderConfigurer =
                c -> {
                    if (!isLocalChannelBuilderConfigured.compareAndSet(false, true)) {
                        fail("localChannelBuilderConfigurer should only be executed once.");
                    }
                };

        BiConsumer<String, ManagedChannelBuilder<?>> remoteChannelBuilderConfigurer =
                (h, c) -> fail("remoteChannelBuilderConfigurer should not be executed.");

        ManagedChannelBuilder<?> channelBuilder = mock(ManagedChannelBuilder.class);
        ManagedChannel channel = mock(ManagedChannel.class);

        when(channelBuilder.build()).thenReturn(channel);

        HostHeaderProxyChannelManager proxyChannelManager = new HostHeaderProxyChannelManager(
                localChannelBuilderConfigurer,
                localHostName,
                remoteChannelBuilderConfigurer) {

            @Override
            protected ManagedChannelBuilder<?> createLocalChannelBuilder(String host) {
                assertThat(host).isEqualTo(localHostName);

                return channelBuilder;
            }
        };

        Metadata headers = new Metadata();

        headers.put(HostHeaderProxyChannelManager.HOST_HEADER_KEY, localHostName);

        Channel actualChannel = proxyChannelManager.getChannel(mock(ServerCall.class), headers);

        assertThat(isLocalChannelBuilderConfigured.get()).isTrue();
        assertThat(actualChannel).isSameAs(channel);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getChannelReturnsRemoteChannelWhenGivenRemoteHostHeader() {
        AtomicBoolean isRemoteChannelBuilderConfigured = new AtomicBoolean();
        String localHostName = UUID.randomUUID().toString();
        String remoteHostName = UUID.randomUUID().toString();

        Consumer<ManagedChannelBuilder<?>> localChannelBuilderConfigurer =
                c -> fail("localChannelBuilderConfigurer should not be executed.");

        BiConsumer<String, ManagedChannelBuilder<?>> remoteChannelBuilderConfigurer =
                (h, c) -> {
                    if (!isRemoteChannelBuilderConfigured.compareAndSet(false, true)) {
                        fail("remoteChannelBuilderConfigurer should only be executed once.");
                    }

                    assertThat(h).isEqualTo(remoteHostName);
                };


        ManagedChannelBuilder<?> channelBuilder = mock(ManagedChannelBuilder.class);
        ManagedChannel channel = mock(ManagedChannel.class);

        when(channelBuilder.build()).thenReturn(channel);

        HostHeaderProxyChannelManager proxyChannelManager = new HostHeaderProxyChannelManager(
                localChannelBuilderConfigurer,
                localHostName,
                remoteChannelBuilderConfigurer) {

            @Override
            protected ManagedChannelBuilder<?> createRemoteChannelBuilder(String host) {
                assertThat(host).isEqualTo(remoteHostName);

                return channelBuilder;
            }
        };

        Metadata headers = new Metadata();

        headers.put(HostHeaderProxyChannelManager.HOST_HEADER_KEY, remoteHostName);

        Channel actualChannel = proxyChannelManager.getChannel(mock(ServerCall.class), headers);

        assertThat(isRemoteChannelBuilderConfigured.get()).isTrue();
        assertThat(actualChannel).isSameAs(channel);
    }
}
