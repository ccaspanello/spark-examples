package com.github.spark.etl;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;


import java.io.Serializable;
import java.util.HashMap;


public class DataSetExamples implements Serializable {

  private SparkContext sc;
  private SparkSession ss;

  public DataSetExamples( SparkContext sc ) {
    this.sc = sc;
    //  Spark Context shouls set the following see
    //    SparkConf sparkConf = new SparkConf();
    //    sparkConf.set( "spark.eventLog.enabled", "true" );
    //    sparkConf.set( "spark.driver.host", "localhost" );
    //    SparkContext sparkContext = new SparkContext( "local[*]", "SparkTest", sparkConf );
    this.ss = SparkSession.builder()
      .appName("DataSetExamples")
      .getOrCreate();
  }

  public Dataset<Row> createBaseDataSet(String fileName) {
    Dataset<Row> result = this.ss.read()
      .format("com.databricks.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(fileName);
    return result;
  }

  public Dataset<Row> sumByOrderNumberSqlUdf(String fileName) {
    this.ss.udf().register("calcLineItem", (Integer qty, Double price) -> qty * price, DataTypes.DoubleType);
    Dataset<Row> orders = this.createBaseDataSet( fileName );
    return orders.selectExpr("ORDERNUMBER",
                             "QUANTITYORDERED",
                             "PRICEEACH",
                             "calcLineItem(QUANTITYORDERED, PRICEEACH) as LINEITEMCOST" );
  }

  public Dataset<Row> sumByOrderNumberSqlInline(String fileName) {
    Dataset<Row> orders = this.createBaseDataSet( fileName );
    return orders.selectExpr("ORDERNUMBER",
      "QUANTITYORDERED",
      "PRICEEACH",
      "QUANTITYORDERED * PRICEEACH as LINEITEMCOST" );
  }

  public Dataset<OrderNumberTotalsBean> sumByStaticClass(String fileName) {
    Dataset<SalesDataBean> orders = this.ss.read()
      .format("com.databricks.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(fileName)
      .as( Encoders.bean( SalesDataBean.class ) );
      return orders.map(
        ( (MapFunction<SalesDataBean, OrderNumberTotalsBean>) sdb -> {
          OrderNumberTotalsBean ontb = new OrderNumberTotalsBean( );
          ontb.setOrderNumber( sdb.getORDERNUMBER() );
          ontb.setOrderLineNumber( sdb.getORDERLINENUMBER() );
          ontb.setPriceEach( sdb.getPRICEEACH( ) );
          ontb.setQuantityOrdered( sdb.getQUANTITYORDERED() );
          ontb.setTotalCost( sdb.getQUANTITYORDERED() * sdb.getPRICEEACH() );
          return ontb;
        } ),
          Encoders.bean( OrderNumberTotalsBean.class ) );
  }

  public Dataset<Row> renameTwoColumns(Dataset<Row> salesData) {
    // Rename "STATUS" to  "new_status"
    // Rename "SALES" to  "new_sales"
    Column sales = salesData.col( "SALES" );
    Column status = salesData.col( "STATUS" );
    return salesData.select( sales.as( "new_status" ),
                             status.as( "new_sales" ) );
  }

  public Dataset<Row> renameColumnsWithSelect(Dataset<Row> salesData) {
    HashMap<String, String> props = new HashMap<>();
    props.put("SALES", "new_sales");
    props.put("STATUS", "new_status");

    // Create Temperary View
    salesData.createOrReplaceTempView( "sales_data" );
    // This could be expensive
    String[] columns = salesData.columns();
    String stmt = "SELECT CAST(ORDERNUMBER as Double) order_number ";
    int i = 1;
    for ( String columnName : columns ) {
      if (i > 0) {
        stmt += ", ";
      }
      i++;

     String replaceValue = props.get(columnName);
     if ( replaceValue != null ) {
       replaceValue = columnName + " AS " + replaceValue;
     }
     else {
       replaceValue = columnName;
     }
     stmt += replaceValue;
    }
    stmt += " FROM sales_data";
    return salesData.sqlContext().sql(stmt);
  }

}

