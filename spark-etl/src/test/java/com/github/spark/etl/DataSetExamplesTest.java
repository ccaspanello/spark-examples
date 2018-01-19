package com.github.spark.etl;

import org.apache.spark.SparkContext;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import javax.inject.Inject;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Guice(modules = GuiceExampleModule.class)
public class DataSetExamplesTest {
  @Inject
  private SparkContext sc;

  @Test
  void creatDsTest() {
    DataSetExamples dses = new DataSetExamples( sc );
    Dataset<Row> ds = dses.createBaseDataSet( "/tmp/sales_data.csv" );
    // Check Headers
    String cols[] = ds.columns();
    assertEquals( cols[0], "ORDERNUMBER" );
    assertEquals( cols[1], "QUANTITYORDERED" );
    assertEquals( cols[2], "PRICEEACH" );

    // Check the first row
    Row head = ds.head();
    assertEquals( Integer.valueOf( head.get( 0 ).toString( ) ),
      new Integer( 10107 ) );
    assertEquals( Integer.valueOf( head.get( 1 ).toString( ) ),
      new Integer( 30 ) );
    assertEquals( Double.valueOf( head.get( 2 ).toString( ) ),
      new Double( 95.70 ) );
  }

  @Test
  void sumByOrderNumberSqlUdfTest() {
    DataSetExamples dses = new DataSetExamples( sc );
    Dataset<Row> ds = dses.sumByOrderNumberSqlUdf();
    ds.collect();
    ds.show(10);
    assertTrue( true );
  }

  @Test
  void sumByOrderNumberSqlInlineTest() {
    DataSetExamples dses = new DataSetExamples( sc );
    Dataset<Row> ds = dses.sumByOrderNumberSqlInline();
    ds.collect();
    ds.show(10);
    assertTrue( true );
  }

  @Test
  void subByStaticClass() {
    DataSetExamples dses = new DataSetExamples( sc );
    Dataset<OrderNumberTotalsBean> ds = dses.sumByStaticClass( "/tmp/sales_data.csv" );
    ds.collect();
    ds.show(10);
    assertTrue( true );
  }
}
