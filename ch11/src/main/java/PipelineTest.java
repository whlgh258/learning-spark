import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.IntParam;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * Created by wanghl on 17-3-23.
 */
public class PipelineTest {
    public static void main(String[] args) {
        Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
        HashingTF tf = new HashingTF().setNumFeatures(10000).setInputCol(tokenizer.getOutputCol()).setOutputCol("features");
        LogisticRegression lr = new LogisticRegression();
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{new PipelineStage() {
            @Override
            public StructType transformSchema(StructType structType) {
                return null;
            }

            @Override
            public PipelineStage copy(ParamMap paramMap) {
                return null;
            }

            @Override
            public String uid() {
                return null;
            }
        }});

        ParamMap[] paramMaps = new ParamGridBuilder().addGrid(new IntParam("", "", String.valueOf(tf.getNumFeatures())), new int[]{10000, 20000}).addGrid(new IntParam("", "", String.valueOf(lr.getMaxIter())), new int[]{100, 200}).build();
        BinaryClassificationEvaluator eval = new BinaryClassificationEvaluator();
        CrossValidator cv = new CrossValidator().setEstimator(lr).setEstimatorParamMaps(paramMaps).setEvaluator(eval);

        SparkSession session = SparkSession.builder().master("local").appName("SparkSqlTest").getOrCreate();
        Dataset<Row> documents = session.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
        PipelineModel model = pipeline.fit(documents);
        CrossValidatorModel bestModel = cv.fit(documents);
    }
}
