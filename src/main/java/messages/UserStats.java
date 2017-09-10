package messages;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class UserStats {
    private String userId;
    private long sessionStartTimestamp;
    private long sessionEndTimestamp;
    private Map<Integer, Integer> messagesSentPerChannel;

    public UserStats() {
    }

    public UserStats(String userId, long sessionStartTimestamp, long sessionEndTimestamp, Map<Integer, Integer> messagesSentPerChannel) {
        System.out.println("New stats window for user " + userId);

        this.userId = userId;
        this.sessionStartTimestamp = sessionStartTimestamp;
        this.sessionEndTimestamp = sessionEndTimestamp;
        this.messagesSentPerChannel = messagesSentPerChannel;
    }

    public UserStats processEvent(Event event) {
        System.out.println("New event arrived for " + event.getUserId());
        System.out.println("Session before:\n" + toString());

        if (userId == null) {
            System.out.println("Initializing session.\n");

            userId = event.getUserId();
            messagesSentPerChannel = new HashMap<>();
        } else if (!userId.equals(event.getUserId())) {
            System.out.println("[E] Processing event in the wrong session!!!");
            return this;
        }
        int newCount = messagesSentPerChannel.containsKey(event.getChannelId())
                ? messagesSentPerChannel.get(event.getChannelId()) + 1
                : 1;
        messagesSentPerChannel.put(event.getChannelId(), newCount);

        System.out.println("Session after:\n" + toString());
        return this;
    }

    public UserStats combine(UserStats other) {
        if (other == null) {
            System.out.println("Trying to combine " + toString() + " with null...");
            return this;
        }

        if (userId == null) {
            System.out.println("[E] Combining " + other + " with non-initialized session!");
            userId = other.userId;
            messagesSentPerChannel = new HashMap<>(other.messagesSentPerChannel);
            return this;
        }

        System.out.println("Combining " + toString() + " with " + other.toString());
        Map<Integer, Integer> msgCounts = new HashMap<>(messagesSentPerChannel);
        for (Map.Entry<Integer, Integer> otherEntry : other.getMessagesSentPerChannel().entrySet()) {
            if (msgCounts.containsKey(otherEntry.getKey())) {
                msgCounts.merge(otherEntry.getKey(), otherEntry.getValue(), Integer::sum);
            } else {
                msgCounts.put(otherEntry.getKey(), otherEntry.getValue());
            }
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

    @Override
    public String toString() {
        String messages =
                messagesSentPerChannel != null
                        ? messagesSentPerChannel
                        .entrySet()
                        .stream()
                        .map(e -> "   channel: " + e.getKey() + " messages: " + e.getValue() + "\n")
                        .collect(Collectors.joining())
                        : "   no messages";
        return "user id: " + userId + "\n" +
               "session start: " + sessionStartTimestamp + "\n" +
               "session end: " + sessionEndTimestamp + "\n" +
                messages + "\n";
    }
}

