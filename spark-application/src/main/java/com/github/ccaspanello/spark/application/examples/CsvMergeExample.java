package com.github.ccaspanello.spark.application.examples;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stand Alone CSV Merge Example
 *
 * Created by ccaspanello on 12/19/2016.
 */
public class CsvMergeExample {

    private static final Logger LOG = LoggerFactory.getLogger(CsvMergeExample.class);

    public void execute(SparkSession sparkSession, String sInputUser, String sInputApp, String sOutput){

        JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());

        LOG.error("TEST");

        Dataset<Row> inputUser = sparkSession.read().format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load(sInputUser);
        LOG.warn("inputUser: {}", inputUser.count());

        Dataset<Row> inputApp = sparkSession.read().format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load(sInputApp);
        LOG.warn("inputApp: {}", inputApp.count());

        inputUser.join(inputApp, "appId").write().format("com.databricks.spark.csv").save(sOutput);
    }
}
