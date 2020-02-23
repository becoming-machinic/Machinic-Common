package com.becomingmachinic.kafka.collections.extensions;

public class JsonUtils {
	
	public static boolean isGsonPresent() {
		try {
			Class.forName("com.google.gson.GsonBuilder");
			return true;
		} catch (Throwable ex) {
			return false;
		}
	}
	
	public static boolean isJacksonPresent() {
		try {
			Class.forName("com.fasterxml.jackson.databind.ObjectMapper");
			return true;
		} catch (Throwable ex) {
			return false;
		}
	}
}
