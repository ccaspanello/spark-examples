package com.github.spark.etl;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


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

  public Dataset<Row> metaDataExample(String fileName) {
    // This is not the most efficent way to do this but this captures most of the syntax we will need
    // Mimics getting JavaRDD row
    JavaRDD<String>  rawOrdersRDD = this.sc.textFile(fileName, 1).toJavaRDD();
    // Nuke Header In real life use the csv tools from spark!!!
    // By doing this we can apply the schema with out errors
    JavaRDD ordersRDD = rawOrdersRDD.mapPartitionsWithIndex(
      (Integer ind, Iterator<String> itr ) -> {
        if ( ind == 0 ) {
          if ( itr.hasNext() ) {
            itr.next();
            return itr;
          } else {
            return itr;
          }
        } else {
          return itr;
        }
      },
      false);

    // Convete to JavaRDD Row
    JavaRDD<Row> rowRDD = ordersRDD.map( (Function<String, Row>) rec -> {
      String [] fields = rec.split(",");
      return RowFactory.create( Integer.valueOf(fields[0]),
                                Integer.valueOf(fields[1]),
                                Double.valueOf(fields[2]),
                                Integer.valueOf(fields[3]),
                                Double.valueOf(fields[4]),
                                fields[5] );
    });

    // Apply schema structure to the JavaRDD
    java.util.List<StructField> fields = new ArrayList<>();
    fields.add( DataTypes.createStructField( "ORDERNUMBER", DataTypes.IntegerType, false ));
    fields.add( DataTypes.createStructField( "QUANTITYORDERED", DataTypes.IntegerType, false ));
    fields.add( DataTypes.createStructField( "PRICEEACH", DataTypes.DoubleType, false ));
    fields.add( DataTypes.createStructField( "ORDERLINENUMBER", DataTypes.IntegerType, false ));
    fields.add( DataTypes.createStructField( "SALES", DataTypes.DoubleType, false ));
    fields.add( DataTypes.createStructField( "STATUS", DataTypes.StringType, false ));
    StructType schema = DataTypes.createStructType( fields );

    Dataset<Row>  ordersDataFrame = this.ss.createDataFrame( rowRDD, schema );

    // Create Tempary view to reference in the SQL
    ordersDataFrame.createOrReplaceTempView("orders");

    // Create Custom function
    this.ss.udf().register("calcLineItem", (Integer qty, Double price) -> qty * price, DataTypes.DoubleType);

    // Spark SQL Reverence Manual: https://docs.databricks.com/spark/latest/spark-sql/index.html
    // Calculate total line Item cost
    String stmt = "SELECT ORDERNUMBER, QUANTITYORDERED, PRICEEACH, ORDERLINENUMBER, SALES, STATUS,";
    stmt += " calcLineItem(QUANTITYORDERED, PRICEEACH) as LINEITEMCOST";
    stmt += " FROM orders";

    // Sum LineItemCost by Status
    return this.ss.sql(stmt).groupBy("STATUS").sum("LINEITEMCOST");

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

  public Dataset<OrderNumberTotalsBean> sumByStaticClassUsingExtendedBean(String fileName) {
    Dataset<SalesDataExtendedBean> orders = this.ss.read()
      .format("com.databricks.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(fileName)
      .as( Encoders.bean( SalesDataExtendedBean.class ) );
    return orders.map(
      ( (MapFunction<SalesDataExtendedBean, OrderNumberTotalsBean>) sdb -> {
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

