import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-22.
 */
public class SpamEmailTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("machine learning");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");


        JavaRDD<String> spam = jsc.textFile("spam.txt");
        JavaRDD<String> normal = jsc.textFile("normal.txt");

        HashingTF tf = new HashingTF(10000);
        JavaRDD<LabeledPoint> posExamples = spam.map(x -> new LabeledPoint(1, tf.transform(Arrays.asList(x.split(" ")))));
        JavaRDD<LabeledPoint> negExamples = normal.map(x -> new LabeledPoint(0, tf.transform(Arrays.asList(x.split(" ")))));

        JavaRDD<LabeledPoint> trainData = posExamples.union(negExamples);
        trainData.cache();

        LogisticRegressionModel model = new LogisticRegressionWithLBFGS().run(trainData.rdd());

        Vector posTest = tf.transform(Arrays.asList("O M G GET cheap stuff by sending money to ...".split(" ")));
        Vector negTest = tf.transform(Arrays.asList("Hi Dad, I started studying Spark the other ...".split(" ")));

        System.out.println("Prediction for positive example: " + model.predict(posTest));
        System.out.println("Prediction for negative example: " + model.predict(negTest));

    }
}
