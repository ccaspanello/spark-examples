package com.github.spark.etl.engine.addsequence;

import com.github.spark.etl.engine.BaseStepMeta;

/**
 * Created by ccaspanello on 1/17/18.
 */
public class AddSequenceMeta extends BaseStepMeta {

  private String columnName;

  public AddSequenceMeta( String name ) {
    super( name );
  }

  //<editor-fold desc="Getters & Setters">
  public String getColumnName() {
    return columnName;
  }

  public void setColumnName( String columnName ) {
    this.columnName = columnName;
  }
  //</editor-fold>
}
