import org.apache.spark.util.AccumulatorV2;

/**
 * Created by wanghl on 17-3-18.
 */
public class MyAccumulatorV2 extends AccumulatorV2<Integer, Integer> {
    private Integer accumulator;

    public MyAccumulatorV2(){
        this.accumulator = 0;
    }

    public MyAccumulatorV2(Integer accumulator){
        this.accumulator = accumulator;
    }

    @Override
    public boolean isZero() {
        return accumulator == 0;
    }

    @Override
    public AccumulatorV2<Integer,Integer> copy() {
        MyAccumulatorV2 other = new MyAccumulatorV2();
        other.accumulator = this.accumulator;
        return other;
    }

    @Override
    public void reset() {
        this.accumulator = 0;
    }

    @Override
    public void merge(AccumulatorV2<Integer, Integer> accumulatorV2) {
        if(accumulatorV2 instanceof MyAccumulatorV2){
            MyAccumulatorV2 o = (MyAccumulatorV2) accumulatorV2;
            this.accumulator += o.accumulator;
        }
    }

    @Override
    public void add(Integer integer) {
        this.accumulator += integer;
    }

    @Override
    public Integer value() {
        return this.accumulator;
    }
}
