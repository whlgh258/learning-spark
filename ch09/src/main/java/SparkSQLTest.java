import java.io.Serializable;
import java.util.ArrayList;

import com.sun.media.jfxmedia.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * Created by wanghl on 17-3-21.
 */
public class SparkSQLTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

//        SQLContext context = new SQLContext(jsc);

        SparkSession session = SparkSession.builder().master("local").appName("SparkSqlTest").getOrCreate();
        session.builder().enableHiveSupport();

        Dataset<Row> input = session.read().json("testweet.json");
        input.show();
        input.printSchema();
        input.createOrReplaceTempView("tweets");
        Dataset<Row> result = session.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");
        result.foreach(x -> System.out.println(x.get(0) + ": " + x.get(1)));

        session.read().parquet("");
        result.write().parquet("");

        ArrayList<HappyPerson> peopleList = new ArrayList<HappyPerson>();
        peopleList.add(new HappyPerson("holden", "coffee"));
        JavaRDD<HappyPerson> happyPeopleRDD = jsc.parallelize(peopleList);
        Dataset<Row> ds = session.createDataFrame(happyPeopleRDD, HappyPerson.class);
        session.udf().register("strLength", (String x) -> x.length(), DataTypes.IntegerType);
        session.sql("select strLength(text) from tweets");
    }
}

class HappyPerson implements Serializable {
    private String name;
    private String favouriteBeverage;
    public HappyPerson() {}
    public HappyPerson(String n, String b) {
        name = n; favouriteBeverage = b;
    }
    public String getName() { return name; }
    public void setName(String n) { name = n; }
    public String getFavouriteBeverage() { return favouriteBeverage; }
    public void setFavouriteBeverage(String b) { favouriteBeverage = b; }
};


















