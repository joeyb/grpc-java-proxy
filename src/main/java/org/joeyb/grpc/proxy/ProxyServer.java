package org.joeyb.grpc.proxy;

import io.grpc.HandlerRegistry;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerMethodDefinition;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * TODO: This will probably end up being a Scone app.
 */
public class ProxyServer {

    /**
     * Runs the proxy server. The expected arguments are the port number that the proxy server will run on and the local
     * server's host.
     *
     * @param args command-line args
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("ERROR: You must give the proxy port and the host name for the local server.");
            System.exit(1);
            return;
        }

        final int port;

        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("ERROR: Failed to parse given port number (" + args[0] + ").");
            System.exit(1);
            return;
        }

        final String localHost = args[1].trim();

        System.out.println("PORT = " + port);
        System.out.println("LOCAL HOST = " + localHost);

        try (HostHeaderProxyChannelManager proxyChannelManager = new HostHeaderProxyChannelManager(
                c -> { },
                localHost,
                (h, c) -> { })) {

            InputStreamMarshaller inputStreamMarshaller = new InputStreamMarshaller();

            Server server = NettyServerBuilder.forPort(port)
                    .fallbackHandlerRegistry(new HandlerRegistry() {
                        @Override
                        public ServerMethodDefinition<?, ?> lookupMethod(
                                String methodName,
                                @Nullable String authority) {
                            return ServerMethodDefinition.create(
                                    MethodDescriptor.create(
                                            MethodDescriptor.MethodType.UNKNOWN,
                                            methodName,
                                            inputStreamMarshaller,
                                            inputStreamMarshaller
                                    ),
                                    new ProxyMethodServerCallHandler(methodName, proxyChannelManager));
                        }
                    })
                    .build();

            server.start();

            try {
                server.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
