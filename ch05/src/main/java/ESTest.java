import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-18.
 */
public class ESTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");
        JobConf jobConf = new JobConf(jsc.hadoopConfiguration());

        jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat");
        jobConf.setOutputCommitter(FileOutputCommitter.class);
        jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, "twitter/tweets");
        jobConf.set(ConfigurationOptions.ES_NODES, "localhost");
        FileOutputFormat.setOutputPath(jobConf, new Path("-"));

        JavaPairRDD<String, Integer> pair = jsc.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("panda", 0),
                new Tuple2<String, Integer>("pink", 3),
                new Tuple2<String, Integer>("pirate", 3),
                new Tuple2<String, Integer>("panda", 1),
                new Tuple2<String, Integer>("pink", 4)));
        pair.saveAsHadoopDataset(jobConf);

        jobConf.set(ConfigurationOptions.ES_RESOURCE_READ, args[1]);
        jobConf.set(ConfigurationOptions.ES_NODES, args[2]);
        JavaPairRDD<Object, MapWritable> currentTweets = jsc.hadoopRDD(jobConf, EsInputFormat.class, Object.class, MapWritable.class);
        currentTweets.map(new Function<Tuple2<Object,MapWritable>, Map<String, String>>() {
            @Override
            public Map<String, String> call(Tuple2<Object, MapWritable> e) throws Exception {
                Map<String, String> map = new HashMap<String, String>();
                map.put(e._1.toString(), e._2.toString());
                return map;
            }
        });
    }
}
