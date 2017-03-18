import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.AccumulatorV2;

/**
 * Created by wanghl on 17-3-18.
 */
public class AccumulatorTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        MyAccumulatorV2 myAccumulatorV2 = new MyAccumulatorV2(0);
        jsc.sc().register(myAccumulatorV2);

        JavaRDD<String> rdd = jsc.textFile(args[1]);
        JavaRDD<String> callSigns = rdd.flatMap(x -> {
            if(StringUtils.isBlank(x)){
                myAccumulatorV2.add(1);
            }

            return Arrays.asList(x.split(" ")).iterator();
        });

        callSigns.saveAsTextFile("output.txt");
        System.out.println("Blank lines: "+ myAccumulatorV2.value());
    }
}
