package com.github.ccaspanello.reactive.spark.application.step;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by ccaspanello on 12/19/2016.
 */
public interface IStep extends Serializable {

    void setSparkSession(SparkSession spark);

    SparkSession getSparkSession();

    void execute();

    IStepMeta getStepMeta();

    void setIncoming(Set<IStep> incoming);

    void setOutgoing(Set<IStep> outgoing);

    Dataset<Row> getData();
}
