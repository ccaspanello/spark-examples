package com.github.spark.etl.engine.addsequence;

import com.github.spark.etl.engine.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by ccaspanello on 1/17/18.
 */
public class AddSequenceStep extends BaseStep<AddSequenceMeta> {

  public AddSequenceStep( AddSequenceMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    String columnName = getStepMeta().getColumnName();
    Dataset<Row> incomming = getIncoming().stream().findFirst().get().getData();
    Dataset<Row> result = incomming.withColumn( columnName, org.apache.spark.sql.functions.monotonicallyIncreasingId() );
    setData(result);
  }
}
