package com.github.ccaspanello.spark.application.step;

/**
 * Created by ccaspanello on 12/19/2016.
 */
public class BaseStepMeta implements IStepMeta {

    private final String name;

    public BaseStepMeta(String name) {
        this.name = name;
    }

    //<editor-fold desc="Getters & Setters">
    @Override
    public String getName() {
        return name;
    }
    //</editor-fold>
}
