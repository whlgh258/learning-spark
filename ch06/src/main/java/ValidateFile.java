import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-18.
 */
public class ValidateFile {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        MyAccumulatorV2 myAccumulatorV2 = new MyAccumulatorV2(0);
        MyAccumulatorV2 validSignCount = new MyAccumulatorV2(0);
        MyAccumulatorV2 invalidSignCount = new MyAccumulatorV2(0);

        JavaRDD<String> rdd = jsc.textFile(args[1]);
        JavaRDD<String> callSigns = rdd.flatMap(x -> {
            if(StringUtils.isBlank(x)){
                myAccumulatorV2.add(1);
            }

            return Arrays.asList(x.split(" ")).iterator();
        });

        JavaRDD<String> validSigns = callSigns.filter(x -> {
            Pattern pattern = Pattern.compile("\\A\\d?[a-zA-Z]{1,2}\\d{1,4}[a-zA-Z]{1,3}\\Z");
            if(pattern.matcher(x).matches()){
                validSignCount.add(1);
                return true;
            }
            else {
                invalidSignCount.add(1);
                return false;
            }
        });

        JavaPairRDD<String, Integer> contactCount = validSigns.mapToPair(x -> new Tuple2<String, Integer>(x, 1)).reduceByKey((x, y) -> x + y);

        if (invalidSignCount.value() < 0.1 * validSignCount.value()) {
            contactCount.saveAsTextFile("/contactCount");
        }
        else{
            System.out.println("Too many errors: " + invalidSignCount.value() + " in %d" + validSignCount.value());
        }
    }
}
