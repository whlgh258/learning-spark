import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Created by wanghl on 17-3-18.
 */
public class ProtocolBufferTest {

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("warn");

        Job job = new Job();
        Configuration conf = job.getConfiguration();
        /*Builder dnaLounge = Venue.newBuilder();
        dnaLounge.setId(1);
        dnaLounge.setName("DNA Lounge");
        dnaLounge.setType(Places.Venue.VenueType.CLUB);

        JavaRDD<Builder> data = jsc.parallelize(Arrays.asList(dnaLounge));
        data.map(x -> {
//            ProtoWritable protoWritable = ProtobufWritable.newInstance(Places.Venue.class);
//            protoWritable.set(x);
            return null;
        });*/
    }
}
