package com.github.ccaspanello.spark.engine.step.datagrid;

import org.apache.spark.sql.types.DataType;

import java.io.Serializable;

/**
 * Created by ccaspanello on 12/20/2016.
 */
public class Column implements Serializable{

    private String name;
    private DataType type;
    boolean nullable;

    public Column(){
    }

    public Column(String name, DataType type){
        this.name = name;
        this.type = type;
    }

    //<editor-fold desc="Getters & Setters">
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }
    //</editor-fold>
}
