package com.github.ccaspanello.spark.application;

import com.github.ccaspanello.spark.application.examples.CsvMergeExample;
import com.github.ccaspanello.spark.application.examples.SparkPi;
import com.github.ccaspanello.spark.application.examples.TransformationExample;
import com.github.ccaspanello.spark.application.examples.WordCount;
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

        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

        switch (args[0].toUpperCase()) {
            case "WORDCOUNT":
                WordCount wordCount = new WordCount();
                wordCount.execute(context, args[1], args[2]);
                break;
            case "SPARKPI":
                SparkPi sparkPi = new SparkPi();
                int slices = Integer.parseInt(args[1]);
                sparkPi.execute(context, slices);
                break;
            case "CSV":
                CsvMergeExample csvMergeExample = new CsvMergeExample();
                csvMergeExample.execute(spark, args[1], args[2], args[3]);
                break;
            case "TRANS":
                TransformationExample transformationExample = new TransformationExample();
                transformationExample.execute(spark, args[1], args[2], args[3]);
                break;
        }

        spark.stop();
    }
}
