import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-16.
 */
public class SortByKeyTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> pair = jsc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, Integer>(100, 0),
                new Tuple2<Integer, Integer>(25, 3),
                new Tuple2<Integer, Integer>(36, 3),
                new Tuple2<Integer, Integer>(5, 1),
                new Tuple2<Integer, Integer>(76, 4)));

        pair.sortByKey().foreach(x -> System.out.println(x));

        pair.sortByKey((x, y) -> String.valueOf(x).compareTo(String.valueOf(y))).foreach(x -> System.out.println(x));
    }
}
