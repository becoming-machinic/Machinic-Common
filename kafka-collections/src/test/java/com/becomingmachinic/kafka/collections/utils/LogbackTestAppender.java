package com.becomingmachinic.kafka.collections.utils;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class LogbackTestAppender extends AppenderBase<ILoggingEvent> {
		private static List<ILoggingEvent> events = Collections.synchronizedList(new ArrayList<>());

		@Override
		protected void append(ILoggingEvent e) {
				events.add(e);
		}

		public static void clear() {
				events.clear();
		}

		public static List<ILoggingEvent> getEvents(String message) {
				List<ILoggingEvent> matchingEvents = new ArrayList<>();
				synchronized (events) {
						for (ILoggingEvent event : events) {
								if (event.getMessage() != null && event.getMessage().equals(message)) {
										matchingEvents.add(event);
								}
						}
				}
				return matchingEvents;
		}

		public static List<ILoggingEvent> getEventsMatching(Pattern pattern) {
				List<ILoggingEvent> matchingEvents = new ArrayList<>();
				synchronized (events) {
						Iterator<ILoggingEvent> it = events.iterator();
						while (it.hasNext()) {
								ILoggingEvent event = it.next();
								if (event.getMessage() != null && pattern.matcher(event.getMessage()).matches()) {
										matchingEvents.add(event);
								}
						}
				}
				return matchingEvents;
		}
}