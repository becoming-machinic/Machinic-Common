package com.becomingmachinic.kafka.streams;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.becomingmachinic.kafka.streams.executor.PartitionExecutorFactory;

public class StreamConfig {
	
	public String getStreamName() {
		return null;
	}
	public List<String> getSourceTopics() {
		return null;
	}
	public long getSourceConsumerMaxPollInterval() {
		return 30000;
	}
	public Map<String, Object> getConsumerConfig() {
		return null;
	}
	public long getCommitInterval() {
		return -1l;
	}
	public PartitionExecutorFactory getPartitionExecutorFactory() {
		return null;
	}
	public AbstractStream.StreamPartitionTaskFactory getPartitionTaskFactory() {
		return null;
	}
	public Pattern getSourceTopicPattern() {
		return null;
	}
}
