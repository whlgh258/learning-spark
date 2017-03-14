import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-13.
 */
public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> input = jsc.textFile("README.md");
        /*JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> result = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });

        result.foreach(s -> System.out.println(s._1 + ": " + s._2));*/

        JavaRDD<String> words = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> pairRDD = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        JavaPairRDD<String, Integer> result = pairRDD.reduceByKey((a, b) -> a + b);
        result.foreach(s -> System.out.println(s._1 + ": " + s._2));

        Map<String, Long> result1 = input.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).countByValue();
        for(Entry<String, Long> ent : result1.entrySet()){
            System.out.println(ent.getKey() + ": " + ent.getValue());
        }
    }
}
