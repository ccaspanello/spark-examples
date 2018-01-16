package com.github.spark.etl.engine.datagrid;

import com.github.spark.etl.engine.BaseStepMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ccaspanello on 12/20/2016.
 */
public class DataGridMeta extends BaseStepMeta {

    public List<Column> columns;
    public List<List<String>> data;

    public DataGridMeta(String name) {
        super(name);
        this.columns = new ArrayList<>();
        this.data = new ArrayList<>();
    }

    //<editor-fold desc="Getters & Setters">
    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public List<List<String>> getData() {
        return data;
    }

    public void setData(List<List<String>> data) {
        this.data = data;
    }
    //</editor-fold>
}
