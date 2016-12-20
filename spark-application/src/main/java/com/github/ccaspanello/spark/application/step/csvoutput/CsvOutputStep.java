package com.github.ccaspanello.spark.application.step.csvoutput;

import com.github.ccaspanello.spark.application.step.BaseStep;

/**
 * CSV Output Step Logic
 *
 * Created by ccaspanello on 12/19/2016.
 */
public class CsvOutputStep extends BaseStep<CsvOutputMeta> {

    public CsvOutputStep(CsvOutputMeta meta) {
        super(meta);
    }

    @Override
    public void execute() {
        getIncoming().stream().findFirst().get().getData().write().csv(getStepMeta().getFilename());
    }
}
