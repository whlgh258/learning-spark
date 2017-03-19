import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.StatCounter;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-19.
 */
public class MapPartitionTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");

        MyAccumulatorV2 myAccumulatorV2 = new MyAccumulatorV2(0);
        MyAccumulatorV2 validSignCount = new MyAccumulatorV2(0);
        MyAccumulatorV2 invalidSignCount = new MyAccumulatorV2(0);

        JavaRDD<String> rdd = jsc.textFile(args[1]);
        JavaRDD<String> callSigns = rdd.flatMap(x -> {
            if(StringUtils.isBlank(x)){
                myAccumulatorV2.add(1);
            }

            return Arrays.asList(x.split(" ")).iterator();
        });

        JavaRDD<String> validSigns = callSigns.filter(x -> {
            Pattern pattern = Pattern.compile("\\A\\d?[a-zA-Z]{1,2}\\d{1,4}[a-zA-Z]{1,3}\\Z");
            if(pattern.matcher(x).matches()){
                validSignCount.add(1);
                return true;
            }
            else {
                invalidSignCount.add(1);
                return false;
            }
        });

        JavaPairRDD<String, CallLog[]> contactsContactLists = validSigns.mapPartitionsToPair(x -> {
            ArrayList<Tuple2<String, CallLog[]>> callsignLogs = new ArrayList<>();
            ArrayList<Tuple2<String, ContentExchange>> requests = new ArrayList<>();
            ObjectMapper mapper = createMapper();
            HttpClient client = new HttpClient();

            try {
                client.start();
                while (x.hasNext()) {
                    requests.add(createRequestForSign(x.next(), client));
                }
                for (Tuple2<String, ContentExchange> signExchange : requests) {
                    callsignLogs.add(fetchResultFromRequest(mapper, signExchange));
                }
            } catch (Exception e) {

            }
            return callsignLogs.iterator();
        });

        System.out.println(StringUtils.join(contactsContactLists.collect(), ","));

        String distScript = "./src/R/finddistance.R";
        String distScriptName = "finddistance.R";
        jsc.addFile(distScript);

        JavaRDD<String> pipeInputs = contactsContactLists.values().map(new VerifyCallLogs()).flatMap(x -> {
            ArrayList<String> latLons = new ArrayList<String>();
            for (CallLog call: x) {
                latLons.add(call.mylat + "," + call.mylong + "," + call.contactlat + "," + call.contactlong);
            }

            return latLons.iterator();
        });

        JavaRDD<String> distances = pipeInputs.pipe(SparkFiles.get(distScriptName));
        System.out.println(StringUtils.join(distances.collect(), ","));

        JavaDoubleRDD distanceDoubles = distances.mapToDouble(x -> Double.parseDouble(x));
        final StatCounter stats = distanceDoubles.stats();
        final Double stddev = stats.stdev();
        final Double mean = stats.mean();
        JavaDoubleRDD reasonableDistances = distanceDoubles.filter(x -> Math.abs(x - mean) <= 3 * stddev);

        System.out.println(StringUtils.join(reasonableDistances.collect(), ","));
    }

    private  static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    private static Tuple2<String, ContentExchange> createRequestForSign(String sign, HttpClient client) throws Exception {
        ContentExchange exchange = new ContentExchange(true);
        exchange.setURL("http://new73s.herokuapp.com/qsos/" + sign + ".json");
        client.send(exchange);
        return new Tuple2(sign, exchange);
    }

    private static Tuple2<String, CallLog[]> fetchResultFromRequest(ObjectMapper mapper, Tuple2<String, ContentExchange> signExchange) {
        String sign = signExchange._1();
        ContentExchange exchange = signExchange._2();
        return new Tuple2(sign, readExchangeCallLog(mapper, exchange));
    }

    private  static CallLog[] readExchangeCallLog(ObjectMapper mapper, ContentExchange exchange) {
        try {
            exchange.waitForDone();
            String responseJson = exchange.getResponseContent();
            return mapper.readValue(responseJson, CallLog[].class);
        } catch (Exception e) {
            return new CallLog[0];
        }
    }
}

class VerifyCallLogs implements Function<CallLog[], CallLog[]> {
    public CallLog[] call(CallLog[] input) {
        ArrayList<CallLog> res = new ArrayList<CallLog>();
        if (input != null) {
            for (CallLog call: input) {
                if (call != null && call.mylat != null && call.mylong != null
                        && call.contactlat != null && call.contactlong != null) {
                    res.add(call);
                }
            }
        }

        return res.toArray(new CallLog[0]);
    }
}
