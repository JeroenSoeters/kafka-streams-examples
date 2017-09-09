package serialization;

import messages.UserStats;

public class UserStatsDeserializer extends JsonDeserializer<UserStats> {
    public UserStatsDeserializer() {
        super(UserStats.class);
    }
}
