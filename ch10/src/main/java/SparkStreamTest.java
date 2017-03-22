import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-21.
 */
public class SparkStreamTest {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
//        JavaSparkContext jsc = new JavaSparkContext(conf);
//        jsc.setLogLevel("warn");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(10000));
        JavaDStream<String> lines = jssc.socketTextStream("localhost", 7777);
        JavaDStream<String> errorLines = lines.filter(x -> x.contains("error"));
        errorLines.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();

        JavaDStream<String> logData = jssc.textFileStream("");
        JavaDStream<ApacheAccessLog> accessLogsDStream = logData.map(x -> ApacheAccessLog.parseFromLogLine(x));
        JavaPairDStream<String, Long> ipDStream = accessLogsDStream.mapToPair(x -> new Tuple2<String, Long>(x.getIpAddress(), 1L));
        JavaPairDStream<String, Long> ipCountsDStream = ipDStream.reduceByKey((x, y) -> x + y);
        JavaPairDStream<String, Long> ipBytesDStream = accessLogsDStream.mapToPair(x -> new Tuple2<String, Long>(x.getIpAddress(), x.getContentSize()));
        JavaPairDStream<String, Long> ipBytesSumDStream = ipBytesDStream.reduceByKey((x, y) -> x + y);
        JavaPairDStream<String, Tuple2<Long, Long>> ipBytesRequestCountDStream = ipCountsDStream.join(ipBytesSumDStream);

        jssc.checkpoint("");
        JavaDStream<ApacheAccessLog> accessLogsWindow = accessLogsDStream.window(new Duration(30000), new Duration(20000));
        JavaDStream<Long> windowCounts = accessLogsWindow.count();

        JavaPairDStream<String, Long> ipAddressPairDStream = accessLogsDStream.mapToPair(x -> new Tuple2<String, Long>(x.getIpAddress(), 1L));
        JavaPairDStream<String, Long> ipCountDStream = ipAddressPairDStream.reduceByKeyAndWindow((x, y) -> x + y, (x, y) -> x -y, new Duration(30), new Duration(10));

        JavaDStream<String> ip = accessLogsDStream.map(x -> x.getIpAddress());
        JavaDStream<Long> requestCount = accessLogsDStream.countByWindow(new Duration(30), new Duration(10));
        JavaPairDStream<String, Long> ipAddressRequestCount = ip.countByValueAndWindow(new Duration(30), new Duration(10));

        JavaPairDStream<Integer, Long> responseCodeCountDStream = accessLogsDStream.mapToPair(x -> new Tuple2<Integer, Long>(x.getResponseCode(), 1L)).updateStateByKey((x, y) -> Optional.of(y.get() + x.size()));

        ipAddressRequestCount.saveAsHadoopFiles("", "");
        JavaPairDStream<Text, LongWritable> writableDStream = ipAddressRequestCount.mapToPair(x -> new Tuple2<>(new Text(x._1), new LongWritable(x._2)));
        writableDStream.saveAsHadoopFiles("", "", Text.class, LongWritable.class, OutputFormat.class);

        ipAddressRequestCount.foreachRDD(x -> x.foreachPartition(y -> y.forEachRemaining(z -> System.out.println(z._1 + ": " + z._2))));

        JavaDStream<String> log = jssc.textFileStream("");
        jssc.fileStream("", Text.class, IntWritable.class, InputFormat.class);

    }

}
