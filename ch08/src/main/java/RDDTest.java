import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-19.
 */
public class RDDTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        JavaRDD<String> input = jsc.textFile("input.txt");
        JavaRDD<String[]> parts = input.map(x -> x.split(" "));
        JavaRDD<String[]> filtered = parts.filter(x -> x.length > 0 && x[0].length() > 0);
        JavaPairRDD<String, Integer> pairs = filtered.mapToPair(x -> new Tuple2<String, Integer>(x[0], 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((x, y) -> x + y);
        List<Tuple2<String, Integer>> result = counts.collect();
        result.forEach(x -> System.out.println(x));
    }
}
