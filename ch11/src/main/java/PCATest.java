import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

/**
 * Created by wanghl on 17-3-23.
 */
public class PCATest {
    public static void main(String[] args) {
        JavaRDD<Vector> vectors = null;
        RowMatrix matrix = new RowMatrix(vectors.rdd());
        Matrix pca = matrix.computePrincipalComponents(2);
        RowMatrix projected = matrix.multiply(pca);
        JavaRDD<Vector> rows = projected.rows().toJavaRDD();
        KMeansModel model = KMeans.train(rows.rdd(), 10, 100);

        SingularValueDecomposition<RowMatrix, Matrix> svd = matrix.computeSVD(20, true, 0.0001);
        RowMatrix u = svd.U();
        Matrix v = svd.V();
        Vector s = svd.s();
    }
}
