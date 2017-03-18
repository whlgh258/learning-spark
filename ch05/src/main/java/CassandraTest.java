import com.datastax.spark.connector.japi.CassandraRow;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

/**
 * Created by wanghl on 17-3-18.
 */
public class CassandraTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost");
        JavaSparkContext sc = new JavaSparkContext("localhost", "basicquerycassandra", conf);
        JavaRDD<CassandraRow> data = javaFunctions(new StreamingContext(conf, new Duration(10))).cassandraTable("test" , "kv");
// Print some basic stats.
        System.out.println(data.mapToDouble(new DoubleFunction<CassandraRow>() {
            public double call(CassandraRow row) { return row.getInt("value"); }
        }).stats());
    }
}
