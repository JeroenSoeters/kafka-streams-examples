package serialization;

import messages.Event;

public class EventDeserializer extends JsonDeserializer<Event> {
    public EventDeserializer() {
        super(Event.class);
    }
}
