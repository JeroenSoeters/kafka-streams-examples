package messages;

import java.util.HashMap;
import java.util.Map;

public class UserStats {
    private String userId;
    private long sessionStartTimestamp;
    private long sessionEndTimestamp;
    private Map<Integer, Integer> messagesSentPerChannel;

    public UserStats() {
        messagesSentPerChannel = new HashMap<>();
    }

    public UserStats(String userId, long sessionStartTimestamp, long sessionEndTimestamp, Map<Integer, Integer> messagesSentPerChannel) {
        this.userId = userId;
        this.sessionStartTimestamp = sessionStartTimestamp;
        this.sessionEndTimestamp = sessionEndTimestamp;
        this.messagesSentPerChannel = messagesSentPerChannel;
    }

    public UserStats processEvent(Event event) {
        messagesSentPerChannel.put(event.getChannelId(), messagesSentPerChannel.get(event.getChannelId()) + 1);

        return this;
    }

    public UserStats combine(UserStats other) {
        Map<Integer, Integer> msgCounts = new HashMap<>(messagesSentPerChannel);
        for (Map.Entry<Integer, Integer> otherEntry : other.getMessagesSentPerChannel().entrySet()) {
            msgCounts.merge(otherEntry.getKey(), otherEntry.getValue(), Integer::sum);
        }

        return new UserStats(
                this.userId,
                this.sessionStartTimestamp,
                other.sessionEndTimestamp,
                msgCounts
        );
    }

    public UserStats setWindowInformation(long start, long end) {
        return new UserStats(userId, start, end, messagesSentPerChannel);
    }

    public String getUserId() {
        return userId;
    }

    public long getSessionStartTimestamp() {
        return sessionStartTimestamp;
    }

    public long getSessionEndTimestamp() {
        return sessionEndTimestamp;
    }

    public Map<Integer, Integer> getMessagesSentPerChannel() {
        return messagesSentPerChannel;
    }

    public int getMessagesSent() {
        return messagesSentPerChannel
                .values()
                .stream()
                .reduce(0, Integer::sum);
    }

    public int getTopChannelId() {
        return messagesSentPerChannel
                .entrySet()
                .stream()
                .sorted((c1, c2) -> Integer.compare(c1.getValue(), c2.getValue()))
                .findFirst()
                .map(c -> c.getKey())
                .orElse(0);
    }

    public int getTopChannelMessagesSent() {
        return messagesSentPerChannel
                .entrySet()
                .stream()
                .sorted((c1, c2) -> Integer.compare(c1.getValue(), c2.getValue()))
                .findFirst()
                .map(c -> c.getKey())
                .orElse(0);
    }
}

