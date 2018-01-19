package com.github.spark.etl;

import com.github.spark.etl.engine.Transformation;
import com.github.spark.etl.engine.addsequence.AddSequenceMeta;
import com.github.spark.etl.engine.addsequence.AddSequenceStep;
import com.github.spark.etl.engine.calculator.CalcFunction;
import com.github.spark.etl.engine.calculator.CalculatorMeta;
import com.github.spark.etl.engine.calculator.CalculatorStep;
import com.github.spark.etl.engine.csvinput.CsvInputMeta;
import com.github.spark.etl.engine.csvinput.CsvInputStep;
import com.github.spark.etl.engine.csvoutput.CsvOutputMeta;
import com.github.spark.etl.engine.csvoutput.CsvOutputStep;
import com.github.spark.etl.engine.datagrid.Column;
import com.github.spark.etl.engine.datagrid.DataGridMeta;
import com.github.spark.etl.engine.datagrid.DataGridStep;
import com.github.spark.etl.engine.lookup.LookupMeta;
import com.github.spark.etl.engine.lookup.LookupStep;
import com.github.spark.etl.engine.rowsfrom.RowsFromResultMeta;
import com.github.spark.etl.engine.rowsfrom.RowsFromResultStep;
import com.github.spark.etl.engine.rowsto.RowsToResultMeta;
import com.github.spark.etl.engine.rowsto.RowsToResultStep;
import com.github.spark.etl.engine.transexecutor.TransExecutorMeta;
import com.github.spark.etl.engine.transexecutor.TransExecutorStep;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Created by ccaspanello on 1/15/18.
 */
@Guice( modules = GuiceExampleModule.class )
public class TransformationRunnerTest {

  private static final Logger LOG = LoggerFactory.getLogger( TransformationRunner.class );

  @Inject
  private SparkContext sc;

  private File output;

  @BeforeTest
  public void beforeTest() {
    try {
      output = File.createTempFile( "test-", "" );
      // Delete file since it is created automatically.
      output.delete();
    } catch ( IOException e ) {
      throw new RuntimeException( "Unexpected error", e );
    }
  }

  @Test
  public void test() throws IOException {

    Transformation transformation = testTransformation();

    TransformationRunner transformationRunner = new TransformationRunner( sc );
    transformationRunner.execute( transformation );

    List<File> csvFiles = Arrays.asList( output.listFiles() ).stream()
      .filter( line -> line.getName().endsWith( ".csv" ) ).collect( Collectors.toList() );

    // TODO Work on assertion
    LOG.info( "OUTPUT - Files: {}", csvFiles.size() );
    LOG.info( "=========================" );
    for ( File file : csvFiles ) {
      List<String> lines = FileUtils.readLines( file );
      for ( String line : lines ) {
        LOG.info( "{}", line );
      }
    }
  }

  // TODO Work on serialization/deserialization of a Transformation object (may have to introduce a
  // TransformationMeta object)
  private Transformation testTransformation() {
    String fileApp = Main.class.getClassLoader().getResource( "input/InputApp.csv" ).getFile();
    String fileUser = Main.class.getClassLoader().getResource( "input/InputUser.csv" ).getFile();

    DataGridMeta dataGrid = new DataGridMeta( "Data Grid" );
    dataGrid.getColumns().add( new Column( "test_string", DataTypes.StringType ) );
    for ( int i = 0; i < 25; i++ ) {
      dataGrid.getData().add( Arrays.asList( UUID.randomUUID().toString() ) );
    }

    AddSequenceMeta addSequence = new AddSequenceMeta( "Add Sequence" );
    addSequence.setColumnName( "test_id" );

    TransExecutorMeta transExecutorMeta = new TransExecutorMeta( "Transformation Executor" );
    transExecutorMeta.setTransformation( subTransformation() );

    CsvOutputMeta csvOutput = new CsvOutputMeta( "Results" );
    csvOutput.setFilename( output.getAbsolutePath() );

    Transformation transformation = new Transformation( "Parent Transformation" );

    // Steps
    DataGridStep dataGridStep = new DataGridStep( dataGrid );
    AddSequenceStep addSequenceStep = new AddSequenceStep( addSequence );
    TransExecutorStep transExecutorStep = new TransExecutorStep( transExecutorMeta );
    CsvOutputStep csvOutputStep = new CsvOutputStep( csvOutput );

    transformation.addStep( dataGridStep );
    transformation.addStep( addSequenceStep );
    transformation.addStep( transExecutorStep );
    transformation.addStep( csvOutputStep );

    // Hops
    transformation.addHop( dataGridStep, addSequenceStep );
    transformation.addHop( addSequenceStep, transExecutorStep );
    transformation.addHop( transExecutorStep, csvOutputStep );
    transformation.addHop( addSequenceStep, csvOutputStep );

    return transformation;
  }

  private Transformation subTransformation() {

    RowsFromResultMeta rowsFromResultMeta = new RowsFromResultMeta( "Incomming Rows" );
    rowsFromResultMeta.getIncommingFields().add( "test_string" );
    rowsFromResultMeta.getIncommingFields().add( "test_id" );

    CalculatorMeta calculatorMeta = new CalculatorMeta( "Calculator" );
    calculatorMeta.setColumnName( "test_id_calc" );
    calculatorMeta.setCalcFunction( CalcFunction.ADD );
    calculatorMeta.setFieldA( "test_id" );
    calculatorMeta.setFieldB( "test_id" );

    RowsToResultMeta rowsToResultMeta = new RowsToResultMeta( "Outgoing Rows" );

    // Steps
    RowsFromResultStep rowsFromResult = new RowsFromResultStep( rowsFromResultMeta );
    CalculatorStep calculatorStep = new CalculatorStep( calculatorMeta );
    RowsToResultStep rowsToResult = new RowsToResultStep( rowsToResultMeta );

    Transformation transformation = new Transformation( "Child Transformation" );

    transformation.addStep( rowsFromResult );
    transformation.addStep( calculatorStep );
    transformation.addStep( rowsToResult );

    // Hops
    transformation.addHop( rowsFromResult, calculatorStep );
    transformation.addHop( calculatorStep, rowsToResult );

    return transformation;
  }

  private Transformation csvTransformation() {
    String fileApp = Main.class.getClassLoader().getResource( "input/InputApp.csv" ).getFile();
    String fileUser = Main.class.getClassLoader().getResource( "input/InputUser.csv" ).getFile();


    DataGridMeta dataGrid = new DataGridMeta( "Data Grid" );
    dataGrid.getColumns().add( new Column( "test_string", DataTypes.StringType ) );

    // Create Meta Model (Note this could be deserialized/serialized into XML to be sent acorss the wire)
    CsvInputMeta inputUser = new CsvInputMeta( "User" );
    inputUser.setFilename( fileUser );

    CsvInputMeta inputApp = new CsvInputMeta( "App" );
    inputApp.setFilename( fileApp );

    LookupMeta lookupMeta = new LookupMeta( "Lookup" );
    lookupMeta.setLeftStep( inputUser.getName() );
    lookupMeta.setRightStep( inputApp.getName() );
    lookupMeta.setField( "appId" );

    CsvOutputMeta outputStepMeta = new CsvOutputMeta( "Output" );
    outputStepMeta.setFilename( output.getAbsolutePath() );

    // Create Steps out of the Step Meta Models
    CsvInputStep inputUserStep = new CsvInputStep( inputUser );
    CsvInputStep inputAppStep = new CsvInputStep( inputApp );
    LookupStep lookupStep = new LookupStep( lookupMeta );
    CsvOutputStep csvOutputStep = new CsvOutputStep( outputStepMeta );

    Transformation transformation = new Transformation( "CSV Transformation" );

    // Steps
    transformation.addStep( inputUserStep );
    transformation.addStep( inputAppStep );
    transformation.addStep( lookupStep );
    transformation.addStep( csvOutputStep );

    // Hops
    transformation.addHop( inputUserStep, lookupStep );
    transformation.addHop( inputAppStep, lookupStep );
    transformation.addHop( lookupStep, csvOutputStep );

    return transformation;
  }
}
