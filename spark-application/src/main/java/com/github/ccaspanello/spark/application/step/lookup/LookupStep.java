package com.github.ccaspanello.spark.application.step.lookup;

import com.github.ccaspanello.spark.application.step.BaseStep;
import com.github.ccaspanello.spark.application.step.IStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Lookup Step Logic
 *
 * Created by ccaspanello on 12/19/2016.
 */
public class LookupStep extends BaseStep<LookupMeta> {

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
        setData(result);
    }
}
