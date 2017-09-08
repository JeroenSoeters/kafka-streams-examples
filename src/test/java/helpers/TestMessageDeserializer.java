package helpers;

import serialization.JsonDeserializer;

public class TestMessageDeserializer extends JsonDeserializer<TestMessage> {
    public TestMessageDeserializer() {
        super(TestMessage.class);
    }
}
