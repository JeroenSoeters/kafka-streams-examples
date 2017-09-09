package messages;

public class Event {
    private long timestamp;
    private String userId;
    private String event;
    private int channelId;
    private int messageId;

    public Event(long timestamp, String userId, String event, int channelId, int messageId) {
        this.timestamp = timestamp;
        this.userId = userId;
        this.event = event;
        this.channelId = channelId;
        this.messageId = messageId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public String getEvent() {
        return event;
    }

    public int getChannelId() {
        return channelId;
    }

    public int getMessageId() {
        return messageId;
    }
}
