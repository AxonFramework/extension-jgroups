package org.axonframework.extensions.jgroups.commandhandling.utils;

import com.thoughtworks.xstream.XStream;
import org.axonframework.serialization.xml.CompactDriver;
import org.axonframework.serialization.xml.XStreamSerializer;

/**
 * Utility providing {@link org.axonframework.serialization.Serializer} instances for testing.
 *
 * @author Lucas Campos
 */
public abstract class TestSerializer {

    private TestSerializer() {
        // Test utility class
    }

    /**
     * Return a {@link XStreamSerializer} using a default {@link XStream} instance with a {@link CompactDriver}.
     *
     * @return a {@link XStreamSerializer} using a default {@link XStream} instance with a {@link CompactDriver}
     */
    public static XStreamSerializer xStreamSerializer() {
        return XStreamSerializer.builder()
                                .xStream(new XStream(new CompactDriver()))
                                .build();
    }
}
