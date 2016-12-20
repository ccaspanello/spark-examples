package com.github.ccaspanello.reactive.spark.application.step.lookup;

import com.github.ccaspanello.reactive.spark.application.step.BaseStep;
import com.github.ccaspanello.reactive.spark.application.step.IStep;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by ccaspanello on 12/19/2016.
 */
public class LookupStep extends BaseStep<LookupMeta> {

    public LookupStep(LookupMeta meta) {
        super(meta);
    }

    @Override
    public void execute() {

        String leftSide = getStepMeta().getLeftSide();
        String rightSide = getStepMeta().getRightSide();
        IStep left = getIncoming().stream().filter(iStep -> iStep.getStepMeta().getName().equals(leftSide)).findFirst().get();
        IStep right = getIncoming().stream().filter(iStep -> iStep.getStepMeta().getName().equals(rightSide)).findFirst().get();

        Dataset<Row> result = left.getData().join(right.getData(),"appId");
        setData(result);

        //inputUser.join(inputApp, "appId").write().format("com.databricks.spark.csv").save(sOutput);
    }
}
