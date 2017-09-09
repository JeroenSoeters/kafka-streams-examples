import helpers.KafkaIntegrationTest;
import helpers.KeyValue;
import helpers.TestMessage;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;


public class ProduceAndConsumeAMessage extends KafkaIntegrationTest {

    @Test
    public void shouldProduceAndComsumeMessages() throws InterruptedException, ExecutionException {
        List<KeyValue<String, TestMessage>> expectedMessageList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            expectedMessageList.add(new KeyValue<>("key" + i, new TestMessage("test-message" + i)));
        }

        produceMessagesToTopic(
                "test_messages",
                expectedMessageList,
                new StringSerializer().getClass().getName(),
                "helpers.TestMessageSerializer");

        List<KeyValue<String, TestMessage>> actualMessageList =
                consumeMessagesFromTopic(
                        "test_messages",
                        new StringDeserializer().getClass().getName(),
                        "helpers.TestMessageDeserializer",
                        10,
                        5000);

        Assert.assertEquals(expectedMessageList.size(), actualMessageList.size());
        Assert.assertEquals(actualMessageList, expectedMessageList);
    }

}
