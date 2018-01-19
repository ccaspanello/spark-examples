package com.github.spark.etl;

import java.io.Serializable;

public class OrderNumberTotalsBean implements Serializable {

  Integer orderNumber;
  Integer quantityOrdered;
  Double  priceEach;
  Integer orderLineNumber;
  Double  totalCost;

  public OrderNumberTotalsBean() {
  }

  public Double getTotalCost() {
    return totalCost;
  }

  public void setTotalCost( Double totalCost ) {
    this.totalCost = totalCost;
  }


  public Integer getOrderNumber() {
    return orderNumber;
  }

  public void setOrderNumber( Integer orderNumber ) {
    this.orderNumber = orderNumber;
  }

  public Integer getQuantityOrdered() {
    return quantityOrdered;
  }

  public void setQuantityOrdered( Integer quantityOrdered ) {
    this.quantityOrdered = quantityOrdered;
  }

  public Double getPriceEach() {
    return priceEach;
  }

  public void setPriceEach( Double priceEach ) {
    this.priceEach = priceEach;
  }

  public Integer getOrderLineNumber() {
    return orderLineNumber;
  }

  public void setOrderLineNumber( Integer orderLineNumber ) {
    this.orderLineNumber = orderLineNumber;
  }

}
