import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-13.
 */
public class AggregateTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> pair = jsc.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("panda", 0),
                                                                               new Tuple2<String, Integer>("pink", 3),
                                                                               new Tuple2<String, Integer>("pirate", 3),
                                                                               new Tuple2<String, Integer>("panda", 1),
                                                                               new Tuple2<String, Integer>("pink", 4)));

        JavaPairRDD<String, Tuple2<Integer, Integer>> mapValuesResult = pair.mapValues(x -> new Tuple2<Integer, Integer>(x, 1));
        JavaPairRDD result = pair.mapValues(x -> new Tuple2<Integer, Integer>(x, 1)).reduceByKey((x, y) -> {
            Tuple2 tuple2 = new Tuple2(x._1 + y._1, x._2 + y._2);
            return tuple2;
        });

        result.foreach(x -> System.out.println(x));

        result.keys().foreach(x -> System.out.println(x));
        result.values().foreach(x -> System.out.println(x));

        JavaPairRDD avgResult = result.mapValues(new Function<Tuple2<Integer, Integer>, Double>() {
            @Override
            public Double call(Tuple2<Integer, Integer> t) throws Exception {
                return t._1 / (double) t._2;
            }
        });

        avgResult.foreach(x -> System.out.println(x));
    }
}
