package com.becomingmachinic.kafka.streams;

public abstract class RunnableTest extends Thread {
	
	private volatile AssertionError exception;
	
	public RunnableTest() {
		this.setDaemon(true);
	}
	
	public abstract void runTest() throws AssertionError;
	
	@Override
	public void run() {
		try {
			this.runTest();
		} catch (AssertionError e) {
			this.exception = e;
		} catch (Exception e) {
			exception = new AssertionError("TestTask failed with exception", e);
		}
	}
	
	public void get(long timeout) throws InterruptedException {
		this.join(timeout);
		if (this.exception != null) {
			throw this.exception;
		}
	}
}
