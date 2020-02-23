package com.becomingmachinic.kafka.streams.executor;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code AutoCloseableThreadLocal} is a {@code java.lang.ThreadLocal} implementation that is specifically for resources that are tied to
 * a thread that need to be cleaned up when the thread is destroyed. This implementation is heavily influenced by
 * {@code jdk.internal.misc.TerminatingThreadLocal} which is part of the jdk.internal.misc package that was made unavailable.
 * <p></p>
 * Unlike TerminatingThreadLocal this implementation only works with {@code PartitionThread}.
 * @param <T> The resource must implement the AutoCloseable interface and should not throw an exception when close is called.
 */
public class AutoCloseableThreadLocal<T extends AutoCloseable> extends ThreadLocal<T> {
	private static final Logger logger = LoggerFactory.getLogger(AutoCloseableThreadLocal.class);
	
	@Override
	public void set(T value) {
		super.set(value);
		register(this);
	}
	
	@Override
	public void remove() {
		super.remove();
		unregister(this);
	}
	
	/**
	 * Invoked by PartitionThread when terminating and this thread-local has an associated
	 * value for the terminating thread (even if that value is null), so that any
	 * native resources maintained by the value can be released.
	 *
	 * @param value current thread's value of this thread-local variable
	 *              (may be null but only if null value was explicitly initialized)
	 */
	protected void threadTerminated(T value) {
		if(value != null){
			try {
				value.close();
			} catch (Exception e) {
				logger.warn(String.format("Close ThreadLocal instance %s failed",value),e);
			}
		}
	}
	
	/*
	 * The following methods is a workaround for the jdk.internal.misc.TerminatingThreadLocal being made unavailable
	 * in JDK version 9 without adding any viable replacement to a non-internal package. A better solution would be to create a new
	 * TerminatingThreadLocal class in a different package and destroy those instances in Thread.exit() like
	 * jdk.internal.misc.TerminatingThreadLocal does.
	 */
	
	/**
	 * Invokes the AutoCloseableThreadLocal's {@link #threadTerminated()} method
	 * on all instances registered in current thread.
	 */
	public static void threadTerminated() {
		for (AutoCloseableThreadLocal<?> ttl : REGISTRY.get()) {
			ttl._threadTerminated();
		}
	}
	
	private void _threadTerminated() { threadTerminated(get()); }
	
	/**
	 * Register given AutoCloseableThreadLocal
	 *
	 * @param tl the ThreadLocal to register
	 */
	public static void register(AutoCloseableThreadLocal<?> tl) {
		REGISTRY.get().add(tl);
	}
	
	/**
	 * Unregister given AutoCloseableThreadLocal
	 *
	 * @param tl the ThreadLocal to unregister
	 */
	private static void unregister(AutoCloseableThreadLocal<?> tl) {
		REGISTRY.get().remove(tl);
	}
	
	/**
	 * a per-thread registry of AutoCloseableThreadLocal(s) that have been registered
	 * but later not unregistered in a particular thread.
	 */
	public static final ThreadLocal<Collection<AutoCloseableThreadLocal<?>>> REGISTRY =
			ThreadLocal.withInitial(() -> Collections.newSetFromMap(new IdentityHashMap<>(4)));
}
