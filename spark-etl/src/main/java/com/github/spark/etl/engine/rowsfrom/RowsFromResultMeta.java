package com.github.spark.etl.engine.rowsfrom;

import com.github.spark.etl.engine.BaseStepMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ccaspanello on 1/18/18.
 */
public class RowsFromResultMeta extends BaseStepMeta {

  private List<String> incommingFields;

  public RowsFromResultMeta( String name ) {
    super( name );
    this.incommingFields = new ArrayList<>();
  }

  //<editor-fold desc="Getters & Setters">
  public List<String> getIncommingFields() {
    return incommingFields;
  }

  public void setIncommingFields( List<String> incommingFields ) {
    this.incommingFields = incommingFields;
  }
  //</editor-fold>
}
