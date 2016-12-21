package com.github.ccaspanello.spark.application;

import com.github.ccaspanello.spark.application.examples.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


/**
 * Entry Point for Spark Job
 *
 * First parameter is the example ID followed by parameters unique to that job.
 *
 * Created by ccaspanello on 12/15/2016.
 */
public class SparkDriver {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkDriver")
                .getOrCreate();



        switch (args[0].toUpperCase()) {
            case "WORDCOUNT":
                WordCount wordCount = new WordCount();
                wordCount.execute(spark, args[1], args[2]);
                break;
            case "SPARKPI":
                SparkPi sparkPi = new SparkPi();
                int slices = Integer.parseInt(args[1]);
                sparkPi.execute(spark, slices);
                break;
            case "CSV":
                CsvMergeExample csvMergeExample = new CsvMergeExample();
                csvMergeExample.execute(spark, args[1], args[2], args[3]);
                break;
            case "TRANS":
                TransformationExample transformationExample = new TransformationExample();
                transformationExample.execute(spark, args[1], args[2], args[3]);
                break;
            case "TRANS2":
                Transformation2Example transformation2Example = new Transformation2Example();
                transformation2Example.execute(spark, args[1]);
                break;
            case "PROCESS":
                ProcessExample processExample = new ProcessExample();
                processExample.execute(spark);
        }

        spark.stop();
    }
}
