package com.github.ccaspanello.reactive.spark.application.step.csvinput;

import com.github.ccaspanello.reactive.spark.application.step.BaseStepMeta;

/**
 * Created by ccaspanello on 12/19/2016.
 */
public class CsvInputMeta extends BaseStepMeta {

    private boolean header;
    private String filename;

    public CsvInputMeta(String name) {
        super(name);
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public boolean isHeader() {
        return header;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }
}
