package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor;

import java.io.Serializable;

import com.google.common.util.concurrent.ListenableFuture;

public interface DistributedFuture<T> extends Serializable, ListenableFuture<T> {

	public boolean setCancelled(boolean mayInterruptIfRunning);

	public String getTaskId();

	public long getCreatedTime();
	
	boolean setException(Throwable throwable);
	
	boolean set(T value);
}
