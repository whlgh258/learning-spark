import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Created by wanghl on 17-3-18.
 */
public class HiveTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

//        HiveContext hiveCtx = new HiveContext(jsc);
//        SparkSession.Builder builder = new SparkSession.Builder();
//        builder.enableHiveSupport();
//        SparkSession session = SparkSession.builder().master("local").appName("Word Count").getOrCreate();
//        SQLContext sqlContext = session.sqlContext();
        SQLContext sqlContext = new SQLContext(jsc);
        Dataset<Row> dataSet= sqlContext.read().json("tweets.json");
        dataSet.createOrReplaceTempView("tweets");
        Dataset<Row> result = sqlContext.sql("SELECT user.name, text FROM tweets");


    }
}
