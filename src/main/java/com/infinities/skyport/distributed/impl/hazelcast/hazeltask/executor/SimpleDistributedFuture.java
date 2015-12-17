package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor;

import com.google.common.util.concurrent.AbstractFuture;

public class SimpleDistributedFuture<T> extends AbstractFuture<T> implements DistributedFuture<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final long createdTime;
	private final IExecutorTopologyService topologyService;
	private final String taskId;


	public SimpleDistributedFuture(IExecutorTopologyService topologyService, String taskId) {
		this.createdTime = System.currentTimeMillis();
		this.topologyService = topologyService;
		this.taskId = taskId;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		if (!this.isCancelled() && topologyService.cancelTask(taskId)) {
			return setCancelled(mayInterruptIfRunning);
		}
		return false;
	}

	@Override
	public boolean setCancelled(boolean mayInterruptIfRunning) {
		return super.cancel(mayInterruptIfRunning);
	}

	@Override
	protected void interruptTask() {

	}

	@Override
	public boolean setException(Throwable e) {
		return super.setException(e);
	}

	@Override
	public boolean set(T value) {
		return super.set(value);
	}

	@Override
	public String getTaskId() {
		return this.taskId;
	}

	@Override
	public long getCreatedTime() {
		return this.createdTime;
	}
}
