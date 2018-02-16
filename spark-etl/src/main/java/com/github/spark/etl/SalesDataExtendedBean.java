package com.github.spark.etl;

import java.io.Serializable;

public class SalesDataExtendedBean implements Serializable {
  private Integer ORDERNUMBER;
  private Integer QUANTITYORDERED;
  private Double PRICEEACH;
  private Integer ORDERLINENUMBER;
  // Sales should be double this is set to generate a hard to find error
  private Integer SALES;
  private String ORDERDATE;
  private String STATUS;
  private Integer QTR_ID;
  private Integer MONTH_ID;
  private Integer YEAR_ID;
  private String PRODUCTLINE;
  private Integer MSRP;
  private String PRODUCTCODE;
  private String CUSTOMERNAME;
  private String PHONE;
  private String ADDRESSLINE1;
  private String ADDRESSLINE2;
  private String CITY;
  private String STATE;
  private String POSTALCODE;
  private String COUNTRY;
  private String TERRITORY;
  private String CONTACTLASTNAME;
  private String CONTACTFIRSTNAME;

  public Integer getORDERNUMBER() {
    return ORDERNUMBER;
  }

  public void setORDERNUMBER( Integer ORDERNUMBER ) {
    this.ORDERNUMBER = ORDERNUMBER;
  }

  public Integer getQUANTITYORDERED() {
    return QUANTITYORDERED;
  }

  public void setQUANTITYORDERED( Integer QUANTITYORDERED ) {
    this.QUANTITYORDERED = QUANTITYORDERED;
  }

  public Double getPRICEEACH() {
    return PRICEEACH;
  }

  public void setPRICEEACH( Double PRICEEACH ) {
    this.PRICEEACH = PRICEEACH;
  }

  public Integer getORDERLINENUMBER() {
    return ORDERLINENUMBER;
  }

  public void setORDERLINENUMBER( Integer ORDERLINENUMBER ) {
    this.ORDERLINENUMBER = ORDERLINENUMBER;
  }

  public Integer getSALES() {
    return SALES;
  }

  public void setSALES( Integer SALES ) {
    this.SALES = SALES;
  }

  public String getORDERDATE() {
    return ORDERDATE;
  }

  public void setORDERDATE( String ORDERDATE ) {
    this.ORDERDATE = ORDERDATE;
  }

  public String getSTATUS() {
    return STATUS;
  }

  public void setSTATUS( String STATUS ) {
    this.STATUS = STATUS;
  }

  public Integer getQTR_ID() {
    return QTR_ID;
  }

  public void setQTR_ID( Integer QTR_ID ) {
    this.QTR_ID = QTR_ID;
  }

  public Integer getMONTH_ID() {
    return MONTH_ID;
  }

  public void setMONTH_ID( Integer MONTH_ID ) {
    this.MONTH_ID = MONTH_ID;
  }

  public Integer getYEAR_ID() {
    return YEAR_ID;
  }

  public void setYEAR_ID( Integer YEAR_ID ) {
    this.YEAR_ID = YEAR_ID;
  }

  public String getPRODUCTLINE() {
    return PRODUCTLINE;
  }

  public void setPRODUCTLINE( String PRODUCTLINE ) {
    this.PRODUCTLINE = PRODUCTLINE;
  }

  public Integer getMSRP() {
    return MSRP;
  }

  public void setMSRP( Integer MSRP ) {
    this.MSRP = MSRP;
  }

  public String getPRODUCTCODE() {
    return PRODUCTCODE;
  }

  public void setPRODUCTCODE( String PRODUCTCODE ) {
    this.PRODUCTCODE = PRODUCTCODE;
  }

  public String getCUSTOMERNAME() {
    return CUSTOMERNAME;
  }

  public void setCUSTOMERNAME( String CUSTOMERNAME ) {
    this.CUSTOMERNAME = CUSTOMERNAME;
  }

  public String getPHONE() {
    return PHONE;
  }

  public void setPHONE( String PHONE ) {
    this.PHONE = PHONE;
  }

  public String getADDRESSLINE1() {
    return ADDRESSLINE1;
  }

  public void setADDRESSLINE1( String ADDRESSLINE1 ) {
    this.ADDRESSLINE1 = ADDRESSLINE1;
  }

  public String getADDRESSLINE2() {
    return ADDRESSLINE2;
  }

  public void setADDRESSLINE2( String ADDRESSLINE2 ) {
    this.ADDRESSLINE2 = ADDRESSLINE2;
  }

  public String getCITY() {
    return CITY;
  }

  public void setCITY( String CITY ) {
    this.CITY = CITY;
  }

  public String getSTATE() {
    return STATE;
  }

  public void setSTATE( String STATE ) {
    this.STATE = STATE;
  }

  public String getPOSTALCODE() {
    return POSTALCODE;
  }

  public void setPOSTALCODE( String POSTALCODE ) {
    this.POSTALCODE = POSTALCODE;
  }

  public String getCOUNTRY() {
    return COUNTRY;
  }

  public void setCOUNTRY( String COUNTRY ) {
    this.COUNTRY = COUNTRY;
  }

  public String getTERRITORY() {
    return TERRITORY;
  }

  public void setTERRITORY( String TERRITORY ) {
    this.TERRITORY = TERRITORY;
  }

  public String getCONTACTLASTNAME() {
    return CONTACTLASTNAME;
  }

  public void setCONTACTLASTNAME( String CONTACTLASTNAME ) {
    this.CONTACTLASTNAME = CONTACTLASTNAME;
  }

  public String getCONTACTFIRSTNAME() {
    return CONTACTFIRSTNAME;
  }

  public void setCONTACTFIRSTNAME( String CONTACTFIRSTNAME ) {
    this.CONTACTFIRSTNAME = CONTACTFIRSTNAME;
  }
}
