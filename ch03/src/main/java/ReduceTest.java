import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaPairRDD;
import org.omg.PortableInterceptor.INACTIVE;
import scala.Tuple2;

/**
 * Created by wanghl on 17-3-13.
 */
public class ReduceTest {

    public static void main(String[] args) {
        AvgCount ret = Arrays.asList(1, 2, 3, 4).stream().reduce(new AvgCount(0, 0), (acc, x) -> {
            AvgCount avgCount = new AvgCount(acc);
            avgCount.total += x;
            avgCount.num += 1;
            return avgCount;
        }, (left, right) -> {
            AvgCount result = new AvgCount(left);
            result.total += right.total;
            result.num += right.num;
            return result;
        });

        System.out.println(ret.avg());

        AvgCount res = Arrays.asList(1, 2, 3, 4).stream().reduce(new AvgCount(0, 0), (acc, x) -> {
            acc.total += x;
            acc.num += 1;
            return acc;
        }, (left, right) -> {
            left.total += right.total;
            left.num += right.num;
            return left;
        });

        System.out.println(res.avg());


    }

    public static <I, O> List<O> map(Stream<I> stream, Function<I, O> mapper) {
        return stream.reduce(new ArrayList<O>(), (acc, x) -> {
            // We are copying data from acc to new list instance. It is very inefficient,
            // but contract of Stream.reduce method requires that accumulator function does
            // not mutate its arguments.
            // Stream.collect method could be used to implement more efficient mutable reduction,
            // but this exercise asks to use reduce method.
            List<O> newAcc = new ArrayList<>(acc);
            newAcc.add(mapper.apply(x));
            return newAcc;
        }, (List<O> left, List<O> right) -> {
            // We are copying left to new list to avoid mutating it.
            List<O> newLeft = new ArrayList<>(left);
            newLeft.addAll(right);
            return newLeft;
        });
    }

    public static AvgCount map1(Stream stream, Function mapper) {
        return (AvgCount) stream.reduce(new AvgCount(0, 0), (acc, x) -> {
            AvgCount avgCount = new AvgCount((AvgCount)acc);
            avgCount.total += (int)x;
            avgCount.num += 1;
            return avgCount;
        }, (left, right) -> {
            AvgCount result = new AvgCount((AvgCount) left);
            result.total += result.total;
            result.num += result.num;
            return result;
        });
    }
}


class AvgCount implements Serializable {
    public int total;
    public int num;

    public AvgCount(int total, int num) {
        this.total = total;
        this.num = num;
    }

    public AvgCount(AvgCount avgCount){
        this.total = avgCount.total;
        this.num = avgCount.num;
    }

    public double avg() {
        return total / (double) num;
    }
}





























