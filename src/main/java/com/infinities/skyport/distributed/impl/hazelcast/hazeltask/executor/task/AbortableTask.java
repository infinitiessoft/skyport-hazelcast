package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task;

public interface AbortableTask<T> extends HazeltaskTask<T> {

	boolean isAbort();

	boolean isInterrupted();

	void interrupt();

	void abort();

}
