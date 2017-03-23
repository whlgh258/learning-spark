import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by wanghl on 17-3-23.
 */
public class TFTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("machine learning");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        String sentence = "hello hello world";
        String[] words = sentence.split(" ");
        HashingTF hashingTF = new HashingTF(10000);
        Vector v = hashingTF.transform(Arrays.asList(words));
        System.out.println(v);

        JavaPairRDD<String, String> wholeText = jsc.wholeTextFiles("");
        JavaRDD<List<String>> result = wholeText.map(new Function<Tuple2<String,String>, List<String>>() {

            @Override
            public List<String> call(Tuple2<String, String> x) throws Exception {
                return Arrays.asList(x._2.split(" "));
            }
        });

        result = wholeText.map(x -> Arrays.asList(x._2.split(" ")));
        JavaRDD<Vector> vectors = hashingTF.transform(result);

        IDF idf = new IDF();
        IDFModel model = idf.fit(vectors);
        JavaRDD<Vector> tfIdfVectors = model.transform(vectors);
    }
}
