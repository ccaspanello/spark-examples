package com.github.ccaspanello.spark.application.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ccaspanello on 12/20/2016.
 */
public class ProcessExample {

    private static Logger LOG = LoggerFactory.getLogger(ProcessExample.class);

    public void execute(SparkSession session) {

        JavaSparkContext context = new JavaSparkContext(session.sparkContext());

        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            list.add(i);
        }

        JavaRDD<Row> rdd = context.parallelize(list, 10).map((Integer i) -> {
                    // DO ANY LOGIC HERE
                    return RowFactory.create(i, "value" + i);
                });

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("key", DataTypes.IntegerType, false),
                DataTypes.createStructField("value", DataTypes.StringType, false)});

        Dataset dataset = session.createDataFrame(rdd, schema);
        dataset.write().csv("file:/c:/temp/output");

        LOG.info("**************************************************");
    }

}
