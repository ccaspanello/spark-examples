package com.github.ccaspanello.reactive.spark.application.step.csvoutput;

import com.github.ccaspanello.reactive.spark.application.step.BaseStepMeta;

/**
 * Created by ccaspanello on 12/19/2016.
 */
public class CsvOutputMeta extends BaseStepMeta {

    private String filename;

    public CsvOutputMeta(String name) {
        super(name);
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }
}
