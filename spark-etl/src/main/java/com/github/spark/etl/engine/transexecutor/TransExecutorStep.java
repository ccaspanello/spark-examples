package com.github.spark.etl.engine.transexecutor;

import com.github.spark.etl.TransformationRunner;
import com.github.spark.etl.engine.BaseStep;
import com.github.spark.etl.engine.IStep;
import com.github.spark.etl.engine.Transformation;
import com.github.spark.etl.engine.rowsfrom.RowsFromResultStep;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.tools.jstat.ExpressionExecuter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by ccaspanello on 1/17/18.
 */
public class TransExecutorStep extends BaseStep<TransExecutorMeta> {

  private static final Logger LOG = LoggerFactory.getLogger( TransExecutorStep.class );

  public TransExecutorStep( TransExecutorMeta meta ) {
    super( meta );
  }

  public boolean bypass = false;

  @Override
  public void execute() {
    Dataset<Row> incomming = getIncoming().stream().findFirst().get().getData();

    if ( bypass ) {
      setData( incomming );
    } else {
      // TODO Convert passthrough to real logic
      Transformation subTrans = getStepMeta().getTransformation();
      RowsFromResultStep fromResultStep =
        (RowsFromResultStep) subTrans.getGraph().vertexSet().stream().filter( new Predicate<IStep>() {
          @Override public boolean test( IStep iStep ) {
            return ( iStep instanceof RowsFromResultStep );
          }
        } ).findFirst().get();

      List<String> fields = fromResultStep.getStepMeta().getIncommingFields();
      String f1 = fields.get( 0 );
      List<String> fx = fields.subList( 1, fields.size() );


      Dataset<Row> result = incomming
        .select( f1, fx.toArray( new String[ fx.size() ] ) )
        .map( new MapFunction<Row, Row>() {
        @Override
        public Row call( Row row ) throws Exception {

          SparkSession ss = new SparkSession(SparkContext.getOrCreate());
          ExpressionEncoder<Row> encoder = RowEncoder.apply(row.schema());

          Dataset<Row> dataset = ss.createDataset( Arrays.asList( row), encoder );
          TransformationRunner transformationRunner = new TransformationRunner( getSparkSession() );
          Dataset<Row> subResult = transformationRunner.executeSubTrans(subTrans, dataset );
          subResult.show();
          return subResult.first();
        }
      }, incomming.exprEnc() );

      setData( result );
    }
  }

}
