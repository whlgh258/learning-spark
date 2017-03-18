import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-18.
 */
public class JDBCTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

//        new JDBCRDD(jsc.sc(), createConnection(), "SELECT * FROM panda WHERE ? <= id AND id <= ?", 1, 3, 1, extractValues());

    }

    public static Connection createConnection(){
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/test?user=holden");
            return conn;

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;

    }

    public static Tuple2<Integer, String> extractValues(ResultSet rs){
        try {
            return new Tuple2<>(rs.getInt(0), rs.getString(1));
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }
}
