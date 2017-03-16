import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-16.
 */
public class PageRankTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaPairRDD<String, List<String>> pair = jsc.parallelizePairs(Arrays.asList(
                new Tuple2<String, List<String>>("a", Arrays.asList("b", "c", "d", "e")),
                new Tuple2<String, List<String>>("b", Arrays.asList("a", "d", "e", "f")),
                new Tuple2<String, List<String>>("c", Arrays.asList("c", "a")),
                new Tuple2<String, List<String>>("d", Arrays.asList("d", "b", "c", "f")),
                new Tuple2<String, List<String>>("e", Arrays.asList("a", "f")),
                new Tuple2<String, List<String>>("f", Arrays.asList("a", "b", "c", "d", "e")))).partitionBy(new HashPartitioner(10)).persist(StorageLevel.MEMORY_ONLY());

        JavaPairRDD<String, Double> rank = pair.mapValues(v -> 1.0);

//        pair.foreach(x -> System.out.println(x));
//        rank.foreach(x -> System.out.println(x));

        for(int i = 0; i < 10; i++){
            JavaPairRDD<String, Tuple2<List<String>, Double>> result = pair.join(rank);
            JavaRDD<Tuple2<String, Tuple2<String, Double>>> flatJavaRDD = result.flatMap(new FlatMapFunction<Tuple2<String,Tuple2<List<String>,Double>>, Tuple2<String, Tuple2<String, Double>>>() {
                @Override
                public Iterator<Tuple2<String, Tuple2<String, Double>>> call(Tuple2<String, Tuple2<List<String>, Double>> e) throws Exception {
                    List<Tuple2<String, Tuple2<String, Double>>> list = new ArrayList<>();
                    String k = e._1;
                    for(String str : e._2._1){
                        Tuple2 tuple = new Tuple2(k, new Tuple2<>(str, e._2._2 / e._2._1.size()));
                        list.add(tuple);
                    }

                    return list.iterator();
                }
            });

            JavaPairRDD<String, Double> contributions = flatJavaRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<String, Double>>, String, Double>() {
                @Override
                public Iterator<Tuple2<String, Double>> call(Tuple2<String, Tuple2<String, Double>> e) throws Exception {
                    List<Tuple2<String, Double>> list = new ArrayList<Tuple2<String, Double>>();
                    Tuple2<String, Double> tuple = new Tuple2<String, Double>(e._2._1, e._2._2);
                    list.add(tuple);

                    return list.iterator();
                }
            });

            rank = contributions.reduceByKey((x, y) -> (x + y)).mapValues(v -> 0.15 + 0.85 * v);
        }

        rank.foreach(x -> System.out.println(x));

        JavaRDD jpr = pair.flatMap(new FlatMapFunction<Tuple2<String,List<String>>, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(Tuple2<String, List<String>> v) throws Exception {
                List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                String k = v._1;
                for(String str : v._2){
                    Tuple2<String, String> tuple = new Tuple2<String, String>(k, str);
                    list.add(tuple);
                }

                return list.iterator();
            }
        });
    }
}

























