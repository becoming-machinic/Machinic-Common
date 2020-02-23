package com.becomingmachinic.kafka.collections.utils;

import java.util.Objects;

public class TestJsonObject {
	private String stringField;
	private Integer integerField;
	private Long longField;
	private Boolean booleanField;
	
	public TestJsonObject() {
	}
	
	public TestJsonObject(String stringField) {
		this.stringField = stringField;
	}
	
	public String getStringField() {
		return stringField;
	}
	public void setStringField(String stringField) {
		this.stringField = stringField;
	}
	public Integer getIntegerField() {
		return integerField;
	}
	public void setIntegerField(Integer integerField) {
		this.integerField = integerField;
	}
	public Long getLongField() {
		return longField;
	}
	public void setLongField(Long longField) {
		this.longField = longField;
	}
	public Boolean getBooleanField() {
		return booleanField;
	}
	public void setBooleanField(Boolean booleanField) {
		this.booleanField = booleanField;
	}
	
	public boolean equals(Object o) {
		if (o != null && o instanceof TestJsonObject) {
			TestJsonObject other = (TestJsonObject) o;
			return Objects.equals(this.stringField, other.stringField) &&
					Objects.equals(this.integerField, other.integerField) &&
					Objects.equals(this.longField, other.longField) &&
					Objects.equals(this.booleanField, other.booleanField);
		}
		return false;
	}
	
	public int hashCode() {
		return Objects.hash(stringField, integerField, longField, booleanField);
	}
}
