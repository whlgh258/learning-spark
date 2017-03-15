import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by wanghl on 17-3-13.
 */
public class AggreateTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4));

        AvgCount ret = rdd.aggregate(new AvgCount(0, 0), (acc, x) -> {
            AvgCount avgCount = new AvgCount(acc);
            avgCount.total += x;
            avgCount.num += 1;
            return avgCount;
        }, (left, right) -> {
            AvgCount result = new AvgCount(left);
            result.total += right.total;
            result.num += right.num;
            return result;
        });

        System.out.println(ret.avg());

        AvgCount res = rdd.aggregate(new AvgCount(0, 0), (AvgCount acc, Integer x) -> {
            acc.total += x;
            acc.num += 1;
            return acc;
        },(AvgCount left, AvgCount right) -> {
            left.total += right.total;
            left.num += right.num;

            return left;
        });

        System.out.println(res.avg());

        AvgCount res1 = rdd.aggregate(new AvgCount(0, 0), (AvgCount acc, Integer x) ->
                      new AvgCount(acc.total + x, acc.num + 1),(AvgCount left, AvgCount right) ->
                      new AvgCount(left.total + right.total, left.num + right.num));
        System.out.println(res1.avg());
    }
}
