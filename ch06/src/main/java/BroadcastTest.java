import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-19.
 */
public class BroadcastTest {

    public static void main(String[] args) throws FileNotFoundException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        final Broadcast<String[]> signPrefixes = jsc.broadcast(loadCallSignTable());

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
        JavaPairRDD<String, Integer> countryCount = contactCount.mapToPair(x -> {
            String sign = x._1();
            String country = lookupCountry(sign, signPrefixes.value());
            return new Tuple2<String, Integer>(country, x._2);
        });

        JavaPairRDD<String, Integer> countryTotalCount = countryCount.reduceByKey((x, y) -> x + y);
        countryTotalCount.saveAsTextFile("/countries.txt");
    }

    private static String[] loadCallSignTable() throws FileNotFoundException {
        Scanner callSignTbl = new Scanner(new File("./files/callsign_tbl_sorted"));
        ArrayList<String> callSignList = new ArrayList<String>();
        while (callSignTbl.hasNextLine()) {
            callSignList.add(callSignTbl.nextLine());
        }
        return callSignList.toArray(new String[0]);
    }

    private static String lookupCountry(String callSign, String[] table) {
        Integer pos = java.util.Arrays.binarySearch(table, callSign);
        if (pos < 0) {
            pos = -pos-1;
        }
        return table[pos].split(",")[1];
    }
}
