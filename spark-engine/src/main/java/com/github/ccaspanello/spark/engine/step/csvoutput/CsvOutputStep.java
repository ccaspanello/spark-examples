package com.github.ccaspanello.spark.engine.step.csvoutput;

import com.github.ccaspanello.spark.engine.step.BaseStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CSV Output Step Logic
 *
 * Created by ccaspanello on 12/19/2016.
 */
public class CsvOutputStep extends BaseStep<CsvOutputMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvOutputStep.class);

    public CsvOutputStep(CsvOutputMeta meta) {
        super(meta);
    }

    @Override
    public void execute() {
        Dataset<Row> result = getIncoming().stream().findFirst().get().getData();
        LOG.info("ROW COUNT for {}: {}", getStepMeta().getName() ,result.count());
//        result.write().text("output-text");
//        result.write().csv(getStepMeta().getFilename());
        result.write().format("com.databricks.spark.csv").option("header", true).save(getStepMeta().getFilename());
    }
}
