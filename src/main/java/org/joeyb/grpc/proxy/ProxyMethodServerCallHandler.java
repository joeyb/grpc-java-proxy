package org.joeyb.grpc.proxy;

import static com.google.common.base.Preconditions.checkNotNull;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;

import java.io.InputStream;

/**
 * {@code ProxyMethodServerCallHandler} is a {@link ServerCallHandler} implementation that proxies a server call and
 * forwards it along to a real server.
 */
public class ProxyMethodServerCallHandler implements ServerCallHandler<InputStream, InputStream> {

    private static final InputStreamMarshaller INPUT_STREAM_MARSHALLER = new InputStreamMarshaller();

    private final String methodName;
    private final ProxyChannelManager proxyChannelManager;

    public ProxyMethodServerCallHandler(String methodName, ProxyChannelManager proxyChannelManager) {
        this.methodName = checkNotNull(methodName, "methodName");
        this.proxyChannelManager = checkNotNull(proxyChannelManager, "proxyChannelManager");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ServerCall.Listener<InputStream> startCall(
            ServerCall<InputStream, InputStream> serverCall,
            Metadata serverHeaders) {

        Channel channel = proxyChannelManager.getChannel(serverCall, serverHeaders);

        // Create the call to the real server. The method type is unknown, so we have to assume bi-directional
        // streaming.
        ClientCall<InputStream, InputStream> clientCall = channel.newCall(
                MethodDescriptor.create(
                        MethodDescriptor.MethodType.UNKNOWN,
                        methodName,
                        INPUT_STREAM_MARSHALLER,
                        INPUT_STREAM_MARSHALLER),
                CallOptions.DEFAULT);

        final ClientCall.Listener<InputStream> clientListener;
        final ServerCall.Listener<InputStream> serverListener;

        // The client listener receives events for the call to the real server.
        clientListener = new ClientCall.Listener<InputStream>() {

            @Override
            public void onHeaders(Metadata headers) {
                // Headers have been received from the real server. Pass them along to the proxy caller.
                serverCall.sendHeaders(headers);
            }

            @Override
            public void onMessage(InputStream message) {
                // A response message has been received from the real server. Pass it along to the proxy caller and
                // request another response message from the real server.
                serverCall.sendMessage(message);
                clientCall.request(1);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                // The real server has completed the request. Pass the status and trailers along to the proxy
                // caller.
                serverCall.close(status, trailers);
            }
        };

        serverListener = new ServerCall.Listener<InputStream>() {

            @Override
            public void onCancel() {
                // The request has been cancelled by the proxy caller. Pass that cancellation along to the call to
                // the real server.
                clientCall.cancel(null, null);
            }

            @Override
            public void onHalfClose() {
                // The proxy caller has indicated that no additional requests will be made. Pass the half close
                // along to the caller of the real server.
                clientCall.halfClose();
            }

            @Override
            public void onMessage(InputStream message) {
                // A request message has been received from the proxy caller. Pass it along to the call to the real
                // server and request another request message from the proxy caller.
                clientCall.sendMessage(message);
                serverCall.request(1);
            }

            @Override
            public void onReady() {
                // The call to the proxy is starting. Initialize the call to the real server and request messages
                // from both the proxy caller and from the real server.
                clientCall.start(clientListener, serverHeaders);
                clientCall.request(1);
                serverCall.request(1);
            }
        };

        return serverListener;
    }
}
