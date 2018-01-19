package com.github.spark.etl.engine.calculator;

import com.esotericsoftware.minlog.Log;
import com.github.spark.etl.engine.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ccaspanello on 1/17/18.
 */
public class CalculatorStep extends BaseStep<CalculatorMeta> {

  private static final Logger LOG = LoggerFactory.getLogger(CalculatorStep.class);

  public CalculatorStep( CalculatorMeta meta ) {
    super( meta );
  }

  @Override
  public void execute() {
    Dataset<Row> incomming = getIncoming().stream().findFirst().get().getData();
    switch ( getStepMeta().getCalcFunction() ) {
      case ADD:
        addFunction( incomming );
        break;
      default:
        throw new RuntimeException( "CalcFunction not supported." );
    }
  }

  private void addFunction( Dataset<Row> incomming ) {
    String colName = getStepMeta().getColumnName();
    String expression = getStepMeta().getFieldA() + "+" + getStepMeta().getFieldB();
    LOG.info("***** ***** ***** ***** ***** Calc: {}", expression);
    Dataset<Row> result = incomming.withColumn( colName, org.apache.spark.sql.functions.expr( expression ) );
    setData( result );
  }
}
