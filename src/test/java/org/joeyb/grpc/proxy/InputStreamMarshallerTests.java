package org.joeyb.grpc.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.junit.Test;

import java.io.InputStream;

public class InputStreamMarshallerTests {

    @Test
    public void parseReturnsTheGivenStreamUnchanged() {
        InputStream input = mock(InputStream.class);

        InputStreamMarshaller marshaller = new InputStreamMarshaller();

        InputStream output = marshaller.parse(input);

        assertThat(output).isSameAs(input);

        verifyZeroInteractions(input);
    }

    @Test
    public void streamReturnsTheGivenValueUnchanged() {
        InputStream input = mock(InputStream.class);

        InputStreamMarshaller marshaller = new InputStreamMarshaller();

        InputStream output = marshaller.stream(input);

        assertThat(output).isSameAs(input);

        verifyZeroInteractions(input);
    }
}
