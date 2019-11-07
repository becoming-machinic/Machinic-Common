package com.becomingmachinic.kafka.collections.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class TestObject {
				private String field1;
				private String field2;

				private TestObject() {
				}
				@JsonCreator
				public TestObject(@JsonProperty("field1") String field1,
						@JsonProperty("field2") String field2) {
						this.field1 = field1;
						this.field2 = field2;
				}

				public String getField1() {
						return field1;
				}
				public String getField2() {
						return field2;
				}

				@Override
				public boolean equals(Object obj) {
						if (obj != null && obj instanceof TestObject) {
								TestObject other = (TestObject) obj;
								return Objects.equals(field1, other.field1) && Objects.equals(field2, other.field2);
						}
						return false;
				}
		}