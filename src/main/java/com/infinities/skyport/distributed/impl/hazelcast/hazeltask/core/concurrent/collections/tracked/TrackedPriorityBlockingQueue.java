package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.collections.tracked;

import java.util.concurrent.PriorityBlockingQueue;

import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.Task;

public class TrackedPriorityBlockingQueue<E extends Task<?>> extends PriorityBlockingQueue<E> implements ITrackedQueue<E> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private volatile Long lastAddedTime;
	private volatile Long lastRemovedTime;
	private static final int DEFAULT_INITIAL_SIZE = 100;


	public TrackedPriorityBlockingQueue() {
		super(DEFAULT_INITIAL_SIZE, new TimeComparator<E>());
	}

	@Override
	public Long getOldestItemTime() {
		E elem = this.peek();
		if (elem != null) {
			return elem.getTimeCreated();
		} else {
			return null;
		}
	}

	@Override
	public boolean offer(E e) {
		boolean r = super.offer(e);
		lastAddedTime = System.currentTimeMillis();
		return r;
	}

	@Override
	public E poll() {
		E e = super.poll();
		lastRemovedTime = System.currentTimeMillis();
		return e;
	}

	@Override
	public boolean remove(Object o) {
		boolean r = super.remove(o);
		lastRemovedTime = System.currentTimeMillis();
		return r;
	}

	@Override
	public Long getLastAddedTime() {
		return lastAddedTime;
	}

	@Override
	public Long getLastRemovedTime() {
		return lastRemovedTime;
	}

}
