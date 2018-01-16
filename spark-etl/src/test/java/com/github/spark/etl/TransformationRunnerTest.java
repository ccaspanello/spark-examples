package com.github.spark.etl;

import com.github.spark.etl.engine.Transformation;
import com.github.spark.etl.engine.csvinput.CsvInputMeta;
import com.github.spark.etl.engine.csvinput.CsvInputStep;
import com.github.spark.etl.engine.csvoutput.CsvOutputMeta;
import com.github.spark.etl.engine.csvoutput.CsvOutputStep;
import com.github.spark.etl.engine.lookup.LookupMeta;
import com.github.spark.etl.engine.lookup.LookupStep;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkContext;
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

  // TODO Work on serialization/deserialization of a Transformation object (may have to introduce a TransformationMeta object)
  private Transformation testTransformation() {
    String fileApp = Main.class.getClassLoader().getResource( "input/InputApp.csv" ).getFile();
    String fileUser = Main.class.getClassLoader().getResource( "input/InputUser.csv" ).getFile();


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

    Transformation transformation = new Transformation();

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
