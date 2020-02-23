package com.becomingmachinic.kafka.streams.executor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Status {
	public static final int RUNNING = 3;
	public static final int SHUTDOWN = 2;
	public static final int TERMINATING = 1;
	public static final int TERMINATED = 0;
	
	private final CountDownLatch statusLatch = new CountDownLatch(RUNNING);
	
	private int getStatus(){
		return (int) this.statusLatch.getCount();
	}
	
	public boolean isTerminated(){
		return this.getStatus() == TERMINATED;
	}
	
	public boolean isExecutingTasks(){
		int status = this.getStatus();
		return status > TERMINATING;
	}
	
	public boolean isAcceptingTasks(){
		return this.getStatus() == RUNNING;
	}
	
	public boolean isShutdown(){
		return this.getStatus() == SHUTDOWN;
	}
	
	public boolean isShuttingDown(){
		return this.getStatus() <= SHUTDOWN;
	}
	
	public synchronized void toShutdown(){
		while (this.getStatus() > SHUTDOWN){
			this.statusLatch.countDown();
		}
	}
	
	public synchronized void toTerminating(){
		while (this.getStatus() > TERMINATING){
			this.statusLatch.countDown();
		}
	}
	
	public void toTerminated(){
		while (this.getStatus() > TERMINATED) {
			this.statusLatch.countDown();
		}
	}
	
	public void awaitTermination() throws InterruptedException {
		this.statusLatch.await();
	}
	
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return this.statusLatch.await(timeout,unit);
	}
}
