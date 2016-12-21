package com.github.ccaspanello.spark.engine.step.datagrid;

import com.github.ccaspanello.spark.engine.step.BaseStep;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ccaspanello on 12/20/2016.
 */
public class DataGridStep extends BaseStep<DataGridMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(DataGridStep.class);

    public DataGridStep(DataGridMeta meta) {
        super(meta);
    }

    @Override
    public void execute() {
        JavaSparkContext context = new JavaSparkContext(getSparkSession().sparkContext());

        List<List<String>> list = getStepMeta().getData();
        List<Column> columns = getStepMeta().getColumns();

        // Define Columns
        List<StructField> fields = new ArrayList<>();
        for(Column column : columns){
            StructField field = DataTypes.createStructField(column.getName(), column.getType(), column.nullable);
            fields.add(field);
        }

        StructType schema = DataTypes.createStructType(fields);

        // Create Row RDD
        JavaRDD<Row> rdd = context.parallelize(list).map((List<String> row) -> {

            // TODO Research if we need to convert the objects based on the DataType or if there is a better way.
            List<Object> convertedRow = new ArrayList<>();
            for(int i=0; i < columns.size(); i++){
                Object value = convert(columns.get(i), row.get(i));
                convertedRow.add(value);
            }

            return RowFactory.create(convertedRow.toArray());
        });

        Dataset<Row> result = getSparkSession().createDataFrame(rdd.collect(), schema);
        LOG.info("ROW COUNT for {}: {}", getStepMeta().getName() ,result.count());
        setData(result);
    }

    // TODO See if there is a better way of doing this using the DataTypes API, if not break out into a utility class.
    private Object convert(Column column, String value) {
        switch(column.getType().typeName()){
            case "string":
                return value;
            case "integer":
                return Integer.valueOf(value);
            default:
                return null;
        }

    }
}
