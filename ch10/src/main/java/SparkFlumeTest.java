import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

/**
 * Created by wanghl on 17-3-22.
 */
public class SparkFlumeTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));

        String receiverHostname = args[0];
        int receiverPort = Integer.parseInt(args[1]);
        JavaDStream<SparkFlumeEvent> events = FlumeUtils.createStream(jssc, receiverHostname, receiverPort);

        JavaDStream<SparkFlumeEvent> events1 = FlumeUtils.createPollingStream(jssc, receiverHostname, receiverPort);

        JavaDStream<String> lines = events.map(e -> new String(e.event().getBody().array(), "utf-8"));

        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate("", () -> new JavaStreamingContext(conf, new Duration(10000)));


    }
}
