import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-18.
 */
public class HadoopFormatTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

//        JavaRDD<Tuple2<String, String>> input = jsc.hadoopFile("path", InputFormat.class, Text.class, Text.class).map(x -> new Tuple2(x._1.toString(), x._2.toString()));
//        JavaPairRDD<String, String> input1 = jsc.hadoopFile("path", InputFormat.class, Text.class, Text.class).mapToPair(x -> new Tuple2(x._1.toString(), x._2.toString()));

//        jsc.newAPIHadoopFile("path", LzoJsonInputFormat.class, LongWritable.class, MapWritable.class, new Configuration());

        JavaPairRDD<String, Integer> rdd = jsc.parallelizePairs(Arrays.asList());
        JavaPairRDD<Text, IntWritable> result = rdd.mapToPair(x -> new Tuple2(new Text(x._1), new IntWritable(x._2)));
        result.saveAsHadoopFile("path", Text.class, IntWritable.class, SequenceFileOutputFormat.class);
    }
}
