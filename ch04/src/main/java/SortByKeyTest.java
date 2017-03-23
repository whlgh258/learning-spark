import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-3-15.
 */
public class SortByKeyTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, Integer> pair = jsc.parallelizePairs(Arrays.asList(
                new Tuple2<Integer, Integer>(100, 1),
                new Tuple2<Integer, Integer>(3, 2),
                new Tuple2<Integer, Integer>(528, 3),
                new Tuple2<Integer, Integer>(65, 9)));

        JavaPairRDD<Integer, Integer> result = pair.sortByKey((a, b) -> String.valueOf(a).compareTo(String.valueOf(b)));
        pair.foreach(x -> System.out.println(x));
//        result.foreach(x -> System.out.println(x));
        Map<Integer, Integer> map = result.collectAsMap();

    }
}
