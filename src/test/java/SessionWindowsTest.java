import helpers.KafkaIntegrationTest;
import helpers.KeyValue;
import messages.Event;
import messages.UserStats;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class SessionWindowsTest extends KafkaIntegrationTest {

    @Test
    public void shouldGenerateUserStatistics() {
        createTopic(Topics.EVENT);
        createTopic(Topics.USER_STATISTICS);

        Instant userOneSessionStart = Instant.now();
        Instant userTwoSessionStart = Instant.now();

        List<Event> userEvents = Arrays.asList(
            new Event(userOneSessionStart.toEpochMilli(), "1", "message_sent", 1, 1),
            new Event(userOneSessionStart.plus(1, ChronoUnit.SECONDS).toEpochMilli(), "1", "message_sent", 2, 2),
            new Event(userOneSessionStart.plus(2, ChronoUnit.SECONDS).toEpochMilli(), "1", "message_sent", 1, 3),
            new Event(userOneSessionStart.plus(4, ChronoUnit.SECONDS).toEpochMilli(), "1", "message_sent", 1, 4),

            new Event(userTwoSessionStart.toEpochMilli(), "2", "message_sent", 1, 5),
            new Event(userTwoSessionStart.plus(1, ChronoUnit.SECONDS).toEpochMilli(), "2", "message_sent", 2, 6),
            new Event(userTwoSessionStart.plus(1, ChronoUnit.SECONDS).toEpochMilli(), "2", "message_sent", 2, 7)
        );

        Map<Integer, Integer> userOneExpectedMessageCounts = new HashMap<>();
        userOneExpectedMessageCounts.put(1, 3);
        userOneExpectedMessageCounts.put(2, 1);

        Map<Integer, Integer> userTwoExpectedMessageCounts = new HashMap<>();
        userTwoExpectedMessageCounts.put(1, 1);
        userTwoExpectedMessageCounts.put(2, 2);

        List<UserStats> expectedUserStats = Arrays.asList(
            new UserStats(
                    "user1",
                    userOneSessionStart.toEpochMilli(),
                    userOneSessionStart.plus(30, ChronoUnit.SECONDS).toEpochMilli(),
                    userOneExpectedMessageCounts),
            new UserStats(
                    "user2",
                    userTwoSessionStart.toEpochMilli(),
                    userTwoSessionStart.plus(30, ChronoUnit.SECONDS).toEpochMilli(),
                    userTwoExpectedMessageCounts)
        );

        try {
            // Start KStreams
            KafkaStreams streams = UserStatisticsTopology.userStatistics();
            streams.cleanUp();
            streams.start();

            Thread.sleep(5000);

            // Produce user events

            produceMessagesToTopic(
                    Topics.EVENT,
                    userEvents.stream().map(e -> new KeyValue<>(e.getUserId(), e)).collect(Collectors.toList()),
                    new StringSerializer().getClass().getName(),
                    "serialization.EventSerializer");

            // Consume user statistics
            List<KeyValue<String, UserStats>> allUserStats =
               consumeMessagesFromTopic(
                    Topics.USER_STATISTICS,
                    new StringDeserializer().getClass().getName(),
                    "serialization.UserStatsDeserializer",
                    2,
                    10000);
            Map<String, UserStats> statsPerUser = new HashMap<>();
            allUserStats.forEach((kv) -> statsPerUser.put(kv.getKey(), kv.getValue()));
            List<UserStats> actualUserStatistics = new ArrayList(statsPerUser.values());

            streams.close();

            Assert.assertEquals(expectedUserStats.size(), actualUserStatistics.size());
            Assert.assertEquals(expectedUserStats, actualUserStatistics);

        } catch (Exception e) {
            Assert.fail("Failed producing messages: " + e.getMessage());
        }
    }
}
