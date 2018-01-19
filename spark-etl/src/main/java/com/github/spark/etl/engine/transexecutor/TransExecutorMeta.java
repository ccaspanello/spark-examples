package com.github.spark.etl.engine.transexecutor;

import com.github.spark.etl.engine.BaseStepMeta;
import com.github.spark.etl.engine.Transformation;

/**
 * Created by ccaspanello on 1/17/18.
 */
public class TransExecutorMeta extends BaseStepMeta {

  public String filename;

  // TODO Replace with reference to transformation
  public Transformation transformation;

  public TransExecutorMeta( String name ) {
    super( name );
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public Transformation getTransformation() {
    return transformation;
  }

  public void setTransformation( Transformation transformation ) {
    this.transformation = transformation;
  }
}
