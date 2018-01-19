package com.github.spark.etl.engine.rowsto;

import com.github.spark.etl.engine.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by ccaspanello on 1/18/18.
 */
public class RowsToResultStep extends BaseStep<RowsToResultMeta> {

  public RowsToResultStep( RowsToResultMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    Dataset<Row> incomming = getIncoming().stream().findFirst().get().getData();
    setData(incomming);
  }

}
