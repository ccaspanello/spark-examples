package com.github.ccaspanello.reactive.spark.application.step.csvoutput;

import com.github.ccaspanello.reactive.spark.application.step.BaseStep;

/**
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
