import java.net.MalformedURLException;
import java.net.URL;

import org.apache.spark.Partitioner;

/**
 * Created by wanghl on 17-3-16.
 */
public class PartitionerTest extends Partitioner {
    private int partitions;

    public void PartitionerTest(){

    }

    public void PartitionerTest(int partitions){
        this.partitions = partitions;
    }


    @Override
    public int numPartitions() {
        return this.partitions;
    }

    @Override
    public int getPartition(Object o) {
        int code = 0;
        String urlStr = (String) o;
        try {
            URL url = new URL(urlStr);
            String host = url.getHost();
            code = host.hashCode() % partitions;
            if(code < 0){
                code += partitions;
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        return code;
    }

    public boolean equals(Object other){
        if(other instanceof PartitionerTest){
            Partitioner o = (PartitionerTest) other;
            if(this.partitions == o.numPartitions()){
                return true;
            }
        }

        return false;
    }

}
