import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by wanghl on 17-3-23.
 */
public class RegressionTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("machine learning");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        SparkSession session = SparkSession.builder().master("local").appName("SparkSqlTest").getOrCreate();
        Dataset<Row> training = session.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
        LinearRegression lr = new LinearRegression().setMaxIter(200).setFitIntercept(true);
        LinearRegressionModel model = lr.fit(training);

        System.out.printf("weights: %s, intercept: %s\n", model.weightCol(), model.intercept());
    }
}
