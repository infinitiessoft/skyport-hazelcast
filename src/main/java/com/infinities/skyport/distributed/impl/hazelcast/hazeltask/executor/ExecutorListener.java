package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor;

import java.io.Serializable;

import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.AbortableTask;

public interface ExecutorListener extends Serializable {

	void beforeExecute(Thread thread, AbortableTask<?> task);

	void afterExecute(AbortableTask<?> task, Throwable exception);

}
