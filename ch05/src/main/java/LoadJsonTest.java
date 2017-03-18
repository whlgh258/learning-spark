import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Created by wanghl on 17-3-18.
 */
public class LoadJsonTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaDoubleRDDTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("warn");
        ObjectMapper mapper = new ObjectMapper();
        JavaRDD<String> json = jsc.textFile("json.file");

        JavaRDD<Person> people = json.map(x -> mapper.readValue(x, Person.class));

        json.mapPartitions(new ParseJson());

        JavaRDD<Person> p = json.mapPartitions(x -> {
            ArrayList list = new ArrayList();
            while(x.hasNext()){
                String line = x.next();
                Person person = mapper.readValue(line, Person.class);
                list.add(person);
            }

            return list.iterator();
        });

        p.filter(x -> x.isLovesPandas()).mapPartitions(new WriteJson()).saveAsTextFile("path");

        people.filter(x -> x.isLovesPandas()).map(x -> {
            return  mapper.writeValueAsString(x);
        }).saveAsTextFile("path");

        p.filter(x -> x.isLovesPandas()).mapPartitions(x -> {
            ArrayList<String> text = new ArrayList<String>();
            while(x.hasNext()){
                Person pp = x.next();
                text.add(mapper.writeValueAsString(pp));
            }

            return text.iterator();
        }).saveAsTextFile("path");
    }
}

class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
    public Iterator<Person> call(Iterator<String> lines) throws Exception {
        ArrayList<Person> people = new ArrayList<Person>();
        ObjectMapper mapper = new ObjectMapper();
        while (lines.hasNext()) {
            String line = lines.next();
            try {
                people.add(mapper.readValue(line, Person.class));
            } catch (Exception e) {
                // skip records on failure
            }
        }
        return people.iterator();
    }
}

class WriteJson implements FlatMapFunction<Iterator<Person>, String> {
    public Iterator<String> call(Iterator<Person> people) throws Exception {
        ArrayList<String> text = new ArrayList<String>();
        ObjectMapper mapper = new ObjectMapper();
        while (people.hasNext()) {
            Person person = people.next();
            text.add(mapper.writeValueAsString(person));
        }
        return text.iterator();
    }
}

class Person{
    private String name;
    private boolean lovesPandas;

    public Person(String name, boolean lovesPandas){
        this.name = name;
        this.lovesPandas = lovesPandas;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isLovesPandas() {
        return lovesPandas;
    }

    public void setLovesPandas(boolean lovesPandas) {
        this.lovesPandas = lovesPandas;
    }
}
