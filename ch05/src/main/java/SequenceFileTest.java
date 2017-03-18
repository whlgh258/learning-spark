import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-18.
 */
public class SequenceFileTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        JavaPairRDD<Text, IntWritable> input = jsc.sequenceFile("path", Text.class, IntWritable.class);

        input.mapToPair(x -> {
            String key = x._1.toString();
            int value = x._2.get();
            return new Tuple2<String, Integer>(key, value);
        });

        input.saveAsHadoopFile("path", Text.class, IntWritable.class, SequenceFileOutputFormat.class);

        JavaPairRDD<String, Integer> data = jsc.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("Panda", 3), new Tuple2<String, Integer>("Kay", 6), new Tuple2<String, Integer>("Snail", 2)));
    }
}
