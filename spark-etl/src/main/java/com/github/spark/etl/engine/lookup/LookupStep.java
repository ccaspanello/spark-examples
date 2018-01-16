package com.github.spark.etl.engine.lookup;

import com.github.spark.etl.engine.BaseStep;
import com.github.spark.etl.engine.IStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lookup Step Logic
 *
 * Created by ccaspanello on 12/19/2016.
 */
public class LookupStep extends BaseStep<LookupMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(LookupStep.class);

    public LookupStep(LookupMeta meta) {
        super(meta);
    }

    @Override
    public void execute() {

        String leftSide = getStepMeta().getLeftStep();
        String rightSide = getStepMeta().getRightStep();
        String field = getStepMeta().getField();

        IStep left = getIncoming().stream().filter(step -> step.getStepMeta().getName().equals(leftSide)).findFirst().get();
        IStep right = getIncoming().stream().filter(step -> step.getStepMeta().getName().equals(rightSide)).findFirst().get();

        Dataset<Row> result = left.getData().join(right.getData(),field);
        LOG.info("ROW COUNT for {}: {}", getStepMeta().getName() ,result.count());
        setData(result);
    }
}
