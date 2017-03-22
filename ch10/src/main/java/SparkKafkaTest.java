import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 * Created by wanghl on 17-3-22.
 */
public class SparkKafkaTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

        String zkQuorum = args[0];
        String group = args[1];
        Map<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("pandas", 1);
        topics.put("logs", 1);
        JavaPairDStream<String, String> input = KafkaUtils.createStream(jssc, zkQuorum, group, topics);
        input.print();
    }
}
