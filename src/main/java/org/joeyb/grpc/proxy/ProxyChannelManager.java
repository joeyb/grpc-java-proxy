package org.joeyb.grpc.proxy;

import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.ServerCall;

import java.io.InputStream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * {@code ProxyChannelManager} defines the interface for fetching the correct {@link Channel} to use for making a
 * proxied server call.
 */
@ThreadSafe
public interface ProxyChannelManager {

    /**
     * Returns the {@link Channel} that should be used when proxying the given server call.
     *
     * @param call The incoming server call that should be proxied.
     * @param headers The headers for the incoming server call that should be proxied.
     */
    Channel getChannel(ServerCall<InputStream, InputStream> call, Metadata headers);
}
