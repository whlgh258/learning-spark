import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-18.
 */
public class WholeTextFilesTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        JavaPairRDD<String, String> pair = jsc.wholeTextFiles("path");

        pair = jsc.parallelizePairs(Arrays.asList(new Tuple2<String, String>("1.txt", "3, 5, 10"), new Tuple2<String, String>("2.txt", "10, 6, 5"), new Tuple2<String, String>("3.txt", "9, 9, 18")));

        JavaPairRDD<String, List<Double>> result = pair.mapValues(x -> {
            String[] y = x.split(",");
            List<Double> list = new ArrayList<>();
            for(String part : y){
                list.add(Double.parseDouble(part));
            }
            return list;
        });

        JavaPairRDD<String, Double> res = result.mapValues(x ->{
            double sum = 0;
            for(int i = 0; i < x.size(); i++){
                sum += x.get(i);
            }

            return sum / x.size();
        });

        res.foreach(x -> System.out.println(x));

        JavaPairRDD<String, Double> ret = pair.mapValues(x -> {
            List<Double> list = new ArrayList<>();
            for(String part : x.split(",")){
                list.add(Double.parseDouble(part));
            }
            return list;
        }).mapValues( y -> {
            double sum = 0;
            for(int i = 0; i < y.size(); i++){
                sum += y.get(i);
            }

            return sum / y.size();
        });

        ret.foreach(x -> System.out.println(x));
    }
}
