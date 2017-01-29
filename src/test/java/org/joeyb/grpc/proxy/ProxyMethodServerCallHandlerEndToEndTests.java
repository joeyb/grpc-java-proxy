package org.joeyb.grpc.proxy;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;

import io.grpc.HandlerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerMethodDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

public class ProxyMethodServerCallHandlerEndToEndTests {

    @Rule
    public final GrpcServerRule realServerRule = new GrpcServerRule();

    private Server proxyServer;
    private ManagedChannel proxyServerChannel;
    private String proxyServerName;

    @Before
    public void startProxyServer() throws IOException {
        proxyServerName = UUID.randomUUID().toString();

        proxyServerChannel = InProcessChannelBuilder.forName(proxyServerName).build();

        InputStreamMarshaller inputStreamMarshaller = new InputStreamMarshaller();
        ProxyChannelManager proxyChannelManager = mock(ProxyChannelManager.class);

        when(proxyChannelManager.getChannel(any(), any())).thenReturn(realServerRule.getChannel());

        proxyServer = InProcessServerBuilder.forName(proxyServerName)
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

        proxyServer.start();
    }

    @After
    public void stopProxyServer() {
        proxyServer.shutdownNow();
        proxyServerChannel.shutdownNow();
        proxyServerName = null;
    }

    @Test
    public void unary() {
        final String inputMessage = UUID.randomUUID().toString();
        final String outputMessage = UUID.randomUUID().toString();

        realServerRule.getServiceRegistry().addService(
                new TestServiceGrpc.TestServiceImplBase() {
                    @Override
                    public void unary(TestRequest request, StreamObserver<TestResponse> responseObserver) {
                        assertThat(request.getMessage()).isEqualTo(inputMessage);

                        responseObserver.onNext(TestResponse.newBuilder().setMessage(outputMessage).build());
                        responseObserver.onCompleted();
                    }
                }.bindService());

        TestServiceGrpc.TestServiceBlockingStub stub = TestServiceGrpc.newBlockingStub(proxyServerChannel);

        TestResponse response = stub.unary(TestRequest.newBuilder().setMessage(inputMessage).build());

        assertThat(response).isNotNull();
        assertThat(response.getMessage()).isEqualTo(outputMessage);
    }

    @Test
    public void clientStreaming() throws InterruptedException {
        final int inputMessageCount = ThreadLocalRandom.current().nextInt(10, 20);
        final List<String> inputMessages = IntStream.range(0, inputMessageCount)
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(toList());
        final String outputMessage = UUID.randomUUID().toString();

        realServerRule.getServiceRegistry().addService(
                new TestServiceGrpc.TestServiceImplBase() {
                    @Override
                    public StreamObserver<TestRequest> clientStreaming(
                            StreamObserver<TestResponse> responseObserver) {

                        ConcurrentLinkedQueue<String> receivedInputMessages = new ConcurrentLinkedQueue<>();

                        return new StreamObserver<TestRequest>() {
                            @Override
                            public void onNext(TestRequest value) {
                                receivedInputMessages.add(value.getMessage());
                            }

                            @Override
                            public void onError(Throwable t) {
                                throw new RuntimeException(t);
                            }

                            @Override
                            public void onCompleted() {
                                assertThat(receivedInputMessages).containsExactlyElementsOf(inputMessages);

                                responseObserver.onNext(
                                        TestResponse.newBuilder().setMessage(outputMessage).build());
                                responseObserver.onCompleted();
                            }
                        };
                    }
                }.bindService());

        TestServiceGrpc.TestServiceStub stub = TestServiceGrpc.newStub(proxyServerChannel);

        AtomicBoolean responseReceived = new AtomicBoolean();
        CountDownLatch responseCompleteLatch = new CountDownLatch(1);

        StreamObserver<TestRequest> requestObserver = stub.clientStreaming(new StreamObserver<TestResponse>() {
            @Override
            public void onNext(TestResponse value) {
                if (!responseReceived.getAndSet(true)) {
                    assertThat(value).isNotNull();
                    assertThat(value.getMessage()).isEqualTo(outputMessage);
                } else {
                    fail("Too many responses received.");
                }
            }

            @Override
            public void onError(Throwable t) {
                throw new RuntimeException(t);
            }

            @Override
            public void onCompleted() {
                responseCompleteLatch.countDown();
            }
        });

        inputMessages.forEach(m -> requestObserver.onNext(TestRequest.newBuilder().setMessage(m).build()));
        requestObserver.onCompleted();

        responseCompleteLatch.await(10, TimeUnit.SECONDS);
    }

    @Test
    public void serverStreaming() {
        final String inputMessage = UUID.randomUUID().toString();
        final int outputMessageCount = ThreadLocalRandom.current().nextInt(10, 20);
        final List<String> outputMessages = IntStream.range(0, outputMessageCount)
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(toList());

        realServerRule.getServiceRegistry().addService(
                new TestServiceGrpc.TestServiceImplBase() {
                    @Override
                    public void serverStreaming(
                            TestRequest request,
                            StreamObserver<TestResponse> responseObserver) {

                        assertThat(request.getMessage()).isEqualTo(inputMessage);

                        outputMessages.forEach(
                                m -> responseObserver.onNext(TestResponse.newBuilder().setMessage(m).build()));
                        responseObserver.onCompleted();
                    }
                }.bindService());

        TestServiceGrpc.TestServiceBlockingStub stub = TestServiceGrpc.newBlockingStub(proxyServerChannel);

        Iterator<TestResponse> responses =
                stub.serverStreaming(TestRequest.newBuilder().setMessage(inputMessage).build());

        assertThat(responses).isNotNull();

        List<String> responseMessages = ImmutableList.copyOf(responses).stream()
                .map(TestResponse::getMessage)
                .collect(toList());

        assertThat(responseMessages).containsExactlyElementsOf(outputMessages);
    }

    @Test
    public void biDirectionalStreaming() throws InterruptedException {
        final int inputMessageCount = ThreadLocalRandom.current().nextInt(10, 20);
        final List<String> inputMessages = IntStream.range(0, inputMessageCount)
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(toList());
        final int outputMessageCount = ThreadLocalRandom.current().nextInt(10, 20);
        final List<String> outputMessages = IntStream.range(0, outputMessageCount)
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(toList());

        realServerRule.getServiceRegistry().addService(
                new TestServiceGrpc.TestServiceImplBase() {
                    @Override
                    public StreamObserver<TestRequest> biDirectionalStreaming(
                            StreamObserver<TestResponse> responseObserver) {

                        ConcurrentLinkedQueue<String> receivedInputMessages = new ConcurrentLinkedQueue<>();

                        return new StreamObserver<TestRequest>() {
                            @Override
                            public void onNext(TestRequest value) {
                                receivedInputMessages.add(value.getMessage());
                            }

                            @Override
                            public void onError(Throwable t) {
                                throw new RuntimeException(t);
                            }

                            @Override
                            public void onCompleted() {
                                assertThat(receivedInputMessages).containsExactlyElementsOf(inputMessages);

                                outputMessages.forEach(
                                        m -> responseObserver.onNext(TestResponse.newBuilder().setMessage(m).build()));
                                responseObserver.onCompleted();
                            }
                        };
                    }
                }.bindService());

        TestServiceGrpc.TestServiceStub stub = TestServiceGrpc.newStub(proxyServerChannel);

        ConcurrentLinkedQueue<String> receivedOutputMessages = new ConcurrentLinkedQueue<>();
        CountDownLatch responseCompleteLatch = new CountDownLatch(1);

        StreamObserver<TestRequest> requestObserver = stub.biDirectionalStreaming(
                new StreamObserver<TestResponse>() {
                    @Override
                    public void onNext(TestResponse value) {
                        receivedOutputMessages.add(value.getMessage());
                    }

                    @Override
                    public void onError(Throwable t) {
                        throw new RuntimeException(t);
                    }

                    @Override
                    public void onCompleted() {
                        responseCompleteLatch.countDown();
                    }
                });

        inputMessages.forEach(m -> requestObserver.onNext(TestRequest.newBuilder().setMessage(m).build()));
        requestObserver.onCompleted();

        responseCompleteLatch.await(20, TimeUnit.SECONDS);

        assertThat(receivedOutputMessages).containsExactlyElementsOf(outputMessages);
    }
}
