package com.chicos.interfaces.common;

public class Pair<T, V> {

  private T first;
  private V second;

  public Pair(T first, V second) {
    this.first = first;
    this.second = second;
  }

  public T getFirst() {
    return first;
  }

  public void setFirst(T first) {
    this.first = first;
  }

  public V getSecond() {
    return second;
  }

  public void setSecond(V second) {
    this.second = second;
  }
  
  public String toString () { // VB: 2019-07-02
	  return new StringBuilder().append("Pair(").append(first).append(",").append(second).append(")").toString();
  }
}
