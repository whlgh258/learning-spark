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

        MyAccumulatorV2 myAccumulatorV2 = new MyAccumulatorV2();
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

class MyAccumulatorV2 extends AccumulatorV2<Integer, Integer> {
    private Integer accumulator;

    @Override
    public boolean isZero() {
        return accumulator == 0;
    }

    @Override
    public AccumulatorV2<Integer,Integer> copy() {
        MyAccumulatorV2 other = new MyAccumulatorV2();
        other.accumulator = this.accumulator;
        return other;
    }

    @Override
    public void reset() {
        this.accumulator = 0;
    }

    @Override
    public void merge(AccumulatorV2<Integer, Integer> accumulatorV2) {
        if(accumulatorV2 instanceof MyAccumulatorV2){
            MyAccumulatorV2 o = (MyAccumulatorV2) accumulatorV2;
            this.accumulator += o.accumulator;
        }
    }

    @Override
    public void add(Integer integer) {
        this.accumulator += integer;
    }

    @Override
    public Integer value() {
        return this.accumulator;
    }

}
