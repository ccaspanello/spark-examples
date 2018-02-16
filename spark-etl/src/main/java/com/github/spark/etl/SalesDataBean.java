package com.github.spark.etl;

import java.io.Serializable;

import static java.lang.System.out;

public class SalesDataBean implements Serializable {
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

  public Double getSALES() {
    return SALES;
  }

  public void setSALES( Double SALES ) {
    this.SALES = SALES;
  }

  public String getSTATUS() {
    return STATUS;
  }

  public void setSTATUS( String STATUS ) {
    this.STATUS = STATUS;
  }

  Integer ORDERNUMBER;
  Integer QUANTITYORDERED;
  Double PRICEEACH;
  Integer ORDERLINENUMBER;
  Double SALES;
  String STATUS;
}

