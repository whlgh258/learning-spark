import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;

/**
 * Created by wanghl on 17-3-23.
 */
public class VectorTest {
    public static void main(String[] args) {
        Vector denseVec1 = Vectors.dense(1.0, 2.0, 3.0);
        Vector denseVec2 = Vectors.dense(new double[] {1.0, 2.0, 3.0});

        Vector sparseVec1 = Vectors.sparse(4, new int[] {0, 2}, new double[]{1.0, 2.0});
    }
}
