package com.github.spark.etl.engine.rowsfrom;

import com.github.spark.etl.engine.BaseStep;

/**
 * Created by ccaspanello on 1/18/18.
 */
public class RowsFromResultStep extends BaseStep<RowsFromResultMeta> {

  public RowsFromResultStep( RowsFromResultMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    // No-op, Sub TransExecutor manages the data in this step by looking into it.
  }

}
