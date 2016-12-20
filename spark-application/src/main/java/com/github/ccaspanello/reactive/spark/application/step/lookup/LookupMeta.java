package com.github.ccaspanello.reactive.spark.application.step.lookup;

import com.github.ccaspanello.reactive.spark.application.step.BaseStepMeta;

/**
 * Created by ccaspanello on 12/19/2016.
 */
public class LookupMeta extends BaseStepMeta {

    public String leftSide;
    public String rightSide;
    public String field;

    public LookupMeta(String name) {
        super(name);
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getLeftSide() {
        return leftSide;
    }

    public void setLeftSide(String leftSide) {
        this.leftSide = leftSide;
    }

    public String getRightSide() {
        return rightSide;
    }

    public void setRightSide(String rightSide) {
        this.rightSide = rightSide;
    }
}
