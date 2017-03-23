import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Normalizer;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by wanghl on 17-3-23.
 */
public class ScaleTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("machine learning");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        List<Vector> vectors = Arrays.asList(Vectors.dense(-2.0, 5.0, 1.0), Vectors.dense(2.0, 0.0, 1.0));
        JavaRDD<Vector> dataset = jsc.parallelize(vectors);
        StandardScaler scaler = new StandardScaler(true, true);
        StandardScalerModel model = scaler.fit(dataset.rdd());
        List<Vector> vs = vectors.stream().map(x -> model.transform(x)).collect(Collectors.toList());
        vs.forEach(System.out::println);

        Normalizer normalizer = new Normalizer();
        List<Vector> vvs = vs.stream().map(x -> normalizer.transform(x)).collect(Collectors.toList());
        vvs.forEach(System.out::println);
    }
}
