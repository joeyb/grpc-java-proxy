package org.joeyb.grpc.proxy;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;

import org.junit.Test;

import java.io.InputStream;
import java.util.UUID;

public class ProxyMethodServerCallHandlerTests {

    @Test
    public void constructorThrowsIfMethodNameIsNull() {
        assertThatThrownBy(() -> new ProxyMethodServerCallHandler(null, mock(ProxyChannelManager.class)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("methodName");
    }

    @Test
    public void constructorThrowsIfProxyChannelManagerIsNull() {
        assertThatThrownBy(() -> new ProxyMethodServerCallHandler(UUID.randomUUID().toString(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("proxyChannelManager");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void clientCallIsConfiguredCorrectly() {
        final Channel channel = mock(Channel.class);
        final String methodName = UUID.randomUUID().toString();
        final ProxyChannelManager proxyChannelManager = mock(ProxyChannelManager.class);
        final ServerCall<InputStream, InputStream> serverCall = mock(ServerCall.class);
        final Metadata serverHeaders = new Metadata();

        when(proxyChannelManager.getChannel(same(serverCall), same(serverHeaders))).thenReturn(channel);

        ProxyMethodServerCallHandler handler = new ProxyMethodServerCallHandler(methodName, proxyChannelManager);

        handler.startCall(serverCall, serverHeaders);

        // TODO: Verify CallOptions.
        verify(channel).newCall(
                argThat(m -> m.getFullMethodName().equals(methodName)
                             && m.getType().equals(MethodDescriptor.MethodType.UNKNOWN)),
                any());
    }
}
