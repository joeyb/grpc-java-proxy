package org.joeyb.grpc.proxy;

import io.grpc.MethodDescriptor;

import java.io.InputStream;

/**
 * {@code InputStreamMarshaller} is an implementation of {@link io.grpc.MethodDescriptor.Marshaller} that just passes
 * through the given {@link InputStream} without making any changes.
 */
public class InputStreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {

    /**
     * Returns the given {@link InputStream} without making any changes.
     *
     * @param stream The input stream to pass through.
     */
    @Override
    public InputStream parse(InputStream stream) {
        return stream;
    }

    /**
     * Returns the given {@link InputStream} without making any changes.
     *
     * @param value The input stream to pass through.
     */
    @Override
    public InputStream stream(InputStream value) {
        return value;
    }
}
