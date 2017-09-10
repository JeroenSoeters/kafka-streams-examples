import messages.Event;
import messages.UserStats;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import serialization.JsonDeserializer;
import serialization.JsonSerializer;
import serialization.WrapperSerde;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class UserStatisticsTopology {
    static public final class EventSerde extends WrapperSerde<Event> {
        public EventSerde() {
            super(new JsonSerializer<Event>(),
                    new JsonDeserializer<Event>(Event.class));
        }
    }

    static public final class UserStatsSerde extends WrapperSerde<UserStats> {
        public UserStatsSerde() {
            super(new JsonSerializer<UserStats>(),
                    new JsonDeserializer<UserStats>(UserStats.class));
        }
    }

    public static KafkaStreams userStatistics() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-stats-stream");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:5000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Event> events =
            builder.stream(
                Serdes.String(),
                new EventSerde(),
                Topics.EVENT);

        KTable<Windowed<String>, UserStats> userStats =
            events
                .groupByKey(Serdes.String(), new EventSerde())
                .aggregate(
                    UserStats::new,
                    (s, event, stats) -> stats.processEvent(event),
                    (k, s1, s2) -> s1.combine(s2),
                    SessionWindows.with(TimeUnit.SECONDS.toMillis(5)).until(TimeUnit.HOURS.toMillis(1)),
                    new UserStatsSerde(),
                    "user-stats-store");

        userStats
           .toStream()
           .filter((window, state) -> state != null)
           .map((window, state) -> new KeyValue<>(window, state.setWindowInformation(window.window().start(), window.window().end())))
           .selectKey((window, stats) -> stats.getUserId())
           .to(Serdes.String(), new UserStatsSerde(), Topics.USER_STATISTICS);

        return new KafkaStreams(builder, props);
    }
}
