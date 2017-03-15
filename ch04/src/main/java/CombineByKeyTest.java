import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-15.
 */
public class CombineByKeyTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> pair = jsc.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("coffee", 1),
                                                                               new Tuple2<String, Integer>("coffee", 2),
                                                                               new Tuple2<String, Integer>("panda", 3),
                                                                               new Tuple2<String, Integer>("coffee", 9)));

        System.out.println(pair.partitions().size());

        JavaPairRDD<String, Tuple2<Integer, Integer>> result = pair.combineByKey(t -> new Tuple2<Integer, Integer>(t, 1),
                                                                                (t, x) -> new Tuple2<Integer, Integer>(t._1 + x, t._2 + 1),
                                                                                (t, x) -> new Tuple2<Integer, Integer>(t._1 + x._1, t._2 + x._2));
        result.foreach(x -> System.out.println(x));
        JavaRDD<Tuple2<String, Double>> avg = result.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Tuple2<Integer, Integer>> x) throws Exception {
                return new Tuple2<String, Double>(x._1, x._2._1 / (double)x._2._2);
            }
        });

        avg.foreach(x -> System.out.println(x));

        JavaRDD<Tuple2<String, Double>> avg1 = result.map((/*Tuple2<String, Tuple2<Integer, Integer>>*/ x) -> new Tuple2<>(x._1, x._2._1 / (double) x._2._2));

        avg1.foreach(x -> System.out.println(x));
    }
}
