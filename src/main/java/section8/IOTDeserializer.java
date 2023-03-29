package section8;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

public class IOTDeserializer implements Deserializer<IOTEvent> {

    @Override
    public void close() {
        Deserializer.super.close();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public IOTEvent deserialize(String s, byte[] bytes) {

        ObjectMapper mapper = new ObjectMapper();
        IOTEvent event = null;
        try {
            event = mapper.readValue(bytes, IOTEvent.class);
        } catch (Exception e) {
            e.toString();
        }
        return event;
    }

    @Override
    public IOTEvent deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

}
