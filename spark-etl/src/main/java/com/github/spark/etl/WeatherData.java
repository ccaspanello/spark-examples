package com.github.spark.etl;

import java.io.Serializable;

/**
 * Created by ccaspanello on 1/16/18.
 */
public class WeatherData implements Serializable {

  private String variableCharacters;
  private String usaf;
  private String wban;
  private String observationDate;
  private String observationTime;
  private String sourceFlag;
  private String latitude;
  private String longitude;
  private String code;
  private String elevation;
  private String callLetter;
  private String qualityControl;

  public WeatherData() {
  }

  public String getVariableCharacters() {
    return variableCharacters;
  }

  public void setVariableCharacters( String variableCharacters ) {
    this.variableCharacters = variableCharacters;
  }

  public String getUsaf() {
    return usaf;
  }

  public void setUsaf( String usaf ) {
    this.usaf = usaf;
  }

  public String getWban() {
    return wban;
  }

  public void setWban( String wban ) {
    this.wban = wban;
  }

  public String getObservationDate() {
    return observationDate;
  }

  public void setObservationDate( String observationDate ) {
    this.observationDate = observationDate;
  }

  public String getObservationTime() {
    return observationTime;
  }

  public void setObservationTime( String observationTime ) {
    this.observationTime = observationTime;
  }

  public String getSourceFlag() {
    return sourceFlag;
  }

  public void setSourceFlag( String sourceFlag ) {
    this.sourceFlag = sourceFlag;
  }

  public String getLatitude() {
    return latitude;
  }

  public void setLatitude( String latitude ) {
    this.latitude = latitude;
  }

  public String getLongitude() {
    return longitude;
  }

  public void setLongitude( String longitude ) {
    this.longitude = longitude;
  }

  public String getCode() {
    return code;
  }

  public void setCode( String code ) {
    this.code = code;
  }

  public String getElevation() {
    return elevation;
  }

  public void setElevation( String elevation ) {
    this.elevation = elevation;
  }

  public String getCallLetter() {
    return callLetter;
  }

  public void setCallLetter( String callLetter ) {
    this.callLetter = callLetter;
  }

  public String getQualityControl() {
    return qualityControl;
  }

  public void setQualityControl( String qualityControl ) {
    this.qualityControl = qualityControl;
  }

  public static WeatherData createFromString( String data ) {
    int offset = 1;
    WeatherData result = new WeatherData();
    result.variableCharacters = data.substring( 1-offset, 4 );
    result.usaf = data.substring( 5-offset, 10 );
    result.wban = data.substring( 11-offset, 15 );
    result.observationDate = data.substring( 16-offset, 23 );
    result.observationTime = data.substring( 24-offset, 27 );
    result.sourceFlag = data.substring( 28-offset, 28 );
    result.latitude = data.substring( 29-offset, 34 );
    result.longitude = data.substring( 35-offset, 41 );
    result.code = data.substring( 42-offset, 46 );
    result.elevation = data.substring( 47-offset, 51 );
    result.callLetter = data.substring( 52-offset, 56 );
    result.qualityControl = data.substring( 57-offset, 60 );
    return result;
  }

  @Override public String toString() {
    final StringBuffer sb = new StringBuffer( "WeatherData{" );
    sb.append( "variableCharacters='" ).append( variableCharacters ).append( '\'' );
    sb.append( ", usaf='" ).append( usaf ).append( '\'' );
    sb.append( ", wban='" ).append( wban ).append( '\'' );
    sb.append( ", observationDate='" ).append( observationDate ).append( '\'' );
    sb.append( ", observationTime='" ).append( observationTime ).append( '\'' );
    sb.append( ", sourceFlag='" ).append( sourceFlag ).append( '\'' );
    sb.append( ", latitude='" ).append( latitude ).append( '\'' );
    sb.append( ", longitude='" ).append( longitude ).append( '\'' );
    sb.append( ", code='" ).append( code ).append( '\'' );
    sb.append( ", elevation='" ).append( elevation ).append( '\'' );
    sb.append( ", callLetter='" ).append( callLetter ).append( '\'' );
    sb.append( ", qualityControl='" ).append( qualityControl ).append( '\'' );
    sb.append( '}' );
    return sb.toString();
  }
}
