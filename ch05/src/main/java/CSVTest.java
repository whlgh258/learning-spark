import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-18.
 */
public class CSVTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        JavaRDD<String> content = jsc.textFile("path");
        JavaRDD<String[]> result = content.map(x -> {
            CSVReader reader = new CSVReader(new StringReader(x));

            return reader.readNext();
        });

        JavaPairRDD<String, String> pair = jsc.wholeTextFiles("path");
        JavaRDD<String[]> ret = pair.flatMap(x -> {
            CSVReader reader = new CSVReader(new StringReader(x._2));
            return reader.readAll().iterator();
        });

        result.map(x -> {
            StringWriter sw = new StringWriter();
            CSVWriter writer = new CSVWriter(sw);
            writer.writeNext(x);

            return writer;
        }).saveAsTextFile("path");

        /*result.mapPartitions(x -> {
            StringWriter sw = new StringWriter();
            CSVWriter writer = new CSVWriter(sw);
            List<String[]> list = new ArrayList<>();
            while(x.hasNext()){
                list.add(x.next());
            }

            writer.writeAll(list);
        });*/

        JavaPairRDD<String, Integer> data = jsc.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("Panda", 3), new Tuple2<String, Integer>("Kay", 6), new Tuple2<String, Integer>("Snail", 2)));
        /*data.map(x -> Arrays.asList(x._1, x._2).toArray()).mapPartitions(String[] x -> {
            StringWriter sw = new StringWriter();
            CSVWriter writer = new CSVWriter(sw);
            List<String[]> list = new ArrayList<>();
            while(x.hasNext()){
                list.add(x.next());
            }

            writer.writeAll(list);
        });*/
    }
}
