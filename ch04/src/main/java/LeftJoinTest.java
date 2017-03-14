import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.omg.PortableInterceptor.INACTIVE;
import scala.Int;
import scala.Tuple2;
import scala.Tuple3;

/**
 * Created by wanghl on 17-3-13.
 */
public class LeftJoinTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);

//        JavaRDD<String> r1 = jsc.parallelize(Arrays.asList("1_2", "3_4", "3_6"));
//        JavaRDD<String> r2 = jsc.parallelize(Arrays.asList("3_9"));

//        JavaPairRDD<Integer, Integer> p1 = r1.mapToPair(s -> new Tuple2<Integer, Integer>(Integer.parseInt(s.split("_")[0]), Integer.parseInt(s.split("_")[1])));
//        JavaPairRDD<Integer, Integer> p2 = r2.mapToPair(s -> new Tuple2<Integer, Integer>(Integer.parseInt(s.split("_")[0]), Integer.parseInt(s.split("_")[1])));

        JavaPairRDD<Integer, Integer> p1 = jsc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(1, 2), new Tuple2<Integer, Integer>(3, 4), new Tuple2<Integer, Integer>(3, 6)));
        JavaPairRDD<Integer, Integer> p2 = jsc.parallelizePairs(Arrays.asList(new Tuple2<Integer, Integer>(3, 9)));

        p1.foreach(s -> System.out.println(s._1 + ": " + s._2));
        p2.foreach(s -> System.out.println(s._1 + ": " + s._2));

        JavaPairRDD<Integer, Integer> reduceResult = p1.reduceByKey((x, y) -> x + y);
        JavaPairRDD<Integer, Iterable<Integer>> groupResult = p1.groupByKey();
        JavaPairRDD<Integer, Integer> mapValueResult = p1.mapValues(x -> x + 1);
        JavaPairRDD<Integer, Integer> flatMapValueResult = p1.flatMapValues(x -> {
            List<Integer> list = new ArrayList<>();
            while(x <= 5){
                list.add(x++);
            }

            return list;
        });
        JavaRDD<Integer> keysResult = p1.keys();
        JavaRDD<Integer> valuesResult = p1.values();
        JavaPairRDD<Integer, Integer> sortedResult = p1.sortByKey();

//        reduceResult.foreach(s -> System.out.println(s._1 + ": " + s._2));
//        groupResult.foreach(s -> System.out.println(s._1 + ": " + s._2));
//        mapValueResult.foreach(s -> System.out.println(s._1 + ": " + s._2));
//        flatMapValueResult.foreach(s -> System.out.println(s._1 + ": " + s._2));
//        keysResult.foreach(s -> System.out.println(s));
//        valuesResult.foreach(s -> System.out.println(s));
//        sortedResult.foreach(s -> System.out.println(s._1 + ": " + s._2));

        JavaPairRDD<Integer, Integer> subtractResult = p1.subtractByKey(p2);
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinResult = p1.join(p2);
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> rightJoinResult = p1.rightOuterJoin(p2);
        JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> leftJoinResult = p1.leftOuterJoin(p2);
        JavaPairRDD<Integer, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroupResult = p1.cogroup(p2);

        subtractResult.foreach(s -> System.out.println(s._1 + ": " + s._2));
        joinResult.foreach(s -> System.out.println(s._1 + ": " + s._2));
        rightJoinResult.foreach(s -> System.out.println(s._1 + ": " + s._2));
        leftJoinResult.foreach(s -> System.out.println(s._1 + ": " + s._2));
        cogroupResult.foreach(s -> System.out.println(s._1 + ": " + s._2));

        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1,2,3,1,2,2,3,4));
        Map<Integer, Long> countByValueResult = rdd.countByValue();
        countByValueResult.forEach((s, t) -> System.out.println(s + ": " + t));

        Map<Integer, Long> pairRDDCountByValueResult = p1.countByKey();
        pairRDDCountByValueResult.forEach((s, t) -> System.out.println(s + ": " + t));

        JavaRDD<Integer> javaRDD = jsc.parallelize(Arrays.asList(1, 2, 3, 4));
        Tuple2<Integer, Integer> tuple2 = javaRDD.aggregate(new Tuple2<Integer, Integer>(0, 0), (Tuple2<Integer, Integer> t, Integer x) -> {
            Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(t._1 + x, t._2 + 1);
            return tuple;
        }, (t, x) -> {
            Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(t._1 + x._1, t._2 + x._2);
            return tuple;
        });

        System.out.println(tuple2._1 / (double)tuple2._2);

        Tuple2<Integer, Integer> tuple3 = javaRDD.aggregate(new Tuple2<>(0, 0), (t, x) -> {
            Tuple2 tuple = new Tuple2(t._1 + x, t._2 + 1);
            return tuple;
        }, (t, x) -> {
            Tuple2 tuple = new Tuple2(t._1 + x._1, t._2 + x._2);
            return tuple;
        });

        System.out.println(tuple3._1 / (double)tuple3._2);
    }
}
