import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

public class JedisStreamConsumerGroupExample {

    public static void main(String[] args) {
        // Replace with your Redis host and port
        String redisHost = "localhost";
        int redisPort = 6379;

        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            String streamKey = "myStream";
            String groupName = "myConsumerGroup";

            // 1. Create the stream if it doesn't exist (optional, but good for testing)
            // You can also add some messages to the stream before creating the group
            jedis.xadd(streamKey, StreamEntryID.NEW_ENTRY, new java.util.HashMap<String, String>() {{
                put("field1", "value1");
            }});

            // 2. Create the consumer group
            // The "$" signifies that the group should start consuming from the latest message
            // `true` for MKSTREAM means it will create the stream if it doesn't exist
            jedis.xgroupCreate(streamKey, groupName, StreamEntryID.LAST_UNCONSUMED_ENTRY, true);

            System.out.println("Consumer group '" + groupName + "' created for stream '" + streamKey + "'");

            // You can now use jedis.xreadgroup() to consume messages from this group
            // ... (further code for consuming messages)

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}