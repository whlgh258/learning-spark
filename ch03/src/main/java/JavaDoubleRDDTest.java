import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by wanghl on 17-3-13.
 */
public class JavaDoubleRDDTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaDoubleRDD result = rdd.mapToDouble( x -> x * x);
        System.out.println(result.mean());
        System.out.println(result.stdev());
    }
}
