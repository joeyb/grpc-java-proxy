package org.joeyb.grpc.proxy;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.ServerCall;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * {@code HostHeaderProxyChannelManager} is an implementation of {@link ProxyChannelManager} that uses a request header
 * in order to route the incoming request to the correct server. The header name used is dictated by
 * {@link HostHeaderProxyChannelManager#HOST_HEADER}. If the header is not specified, then it routes the request to the
 * host that is configured as the "local" host (i.e. the service instance that is paired with this proxy instance).
 */
public class HostHeaderProxyChannelManager implements AutoCloseable, ProxyChannelManager {

    public static final String HOST_HEADER = "Host";
    public static final Metadata.Key<String> HOST_HEADER_KEY = Metadata.Key.of(HOST_HEADER, ASCII_STRING_MARSHALLER);

    @VisibleForTesting
    final Map<String, ManagedChannel> channels;

    @VisibleForTesting
    final Consumer<ManagedChannelBuilder<?>> localChannelBuilderConfigurer;

    @VisibleForTesting
    final String localHostName;

    @VisibleForTesting
    final BiConsumer<String, ManagedChannelBuilder<?>> remoteChannelBuilderConfigurer;

    public HostHeaderProxyChannelManager(
            Consumer<ManagedChannelBuilder<?>> localChannelBuilderConfigurer,
            String localHostName,
            BiConsumer<String, ManagedChannelBuilder<?>> remoteChannelBuilderConfigurer) {

        this.channels = new ConcurrentHashMap<>();

        this.localChannelBuilderConfigurer = checkNotNull(
                localChannelBuilderConfigurer,
                "localChannelBuilderConfigurer");

        this.localHostName = checkNotNull(localHostName, "localHostName");

        this.remoteChannelBuilderConfigurer = checkNotNull(
                remoteChannelBuilderConfigurer,
                "remoteChannelBuilderConfigurer");
    }

    /**
     * Shuts down all {@link Channel} instances that are maintained by this {@link ProxyChannelManager}.
     */
    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdown);
        channels.values().forEach(c -> {
            try {
                c.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                c.shutdownNow();
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Channel getChannel(ServerCall<InputStream, InputStream> call, Metadata headers) {
        String host = headers.containsKey(HOST_HEADER_KEY)
                      ? headers.get(HOST_HEADER_KEY)
                      : localHostName;

        return channels.computeIfAbsent(host, h -> createAndConfigureChannelBuilder(h).build());
    }

    /**
     * Returns a {@link ManagedChannelBuilder} for creating a {@link Channel} to the "local" service (i.e. the service
     * instance that is paired with this proxy instance).
     *
     * <p>TODO: Don't default to plaintext. We should be configuring TLS.
     *
     * @param host the local host name
     */
    protected ManagedChannelBuilder<?> createLocalChannelBuilder(String host) {
        return ManagedChannelBuilder.forTarget(host).usePlaintext(true);
    }

    /**
     * Returns a {@link ManagedChannelBuilder} for creating a {@link Channel} to a "remote" service (i.e. NOT the
     * service instance that is paired with this proxy instance).
     *
     * <p>TODO: Don't default to plaintext. We should be configuring TLS.
     *
     * @param host the remote host name
     */
    protected ManagedChannelBuilder<?> createRemoteChannelBuilder(String host) {
        return ManagedChannelBuilder.forTarget(host).usePlaintext(true);
    }

    private ManagedChannelBuilder<?> createAndConfigureChannelBuilder(String host) {
        if (host.equals(localHostName)) {
            ManagedChannelBuilder<?> builder = createLocalChannelBuilder(host);

            localChannelBuilderConfigurer.accept(builder);

            return builder;
        } else {
            ManagedChannelBuilder<?> builder = createRemoteChannelBuilder(host);

            remoteChannelBuilderConfigurer.accept(host, builder);

            return builder;
        }
    }
}
