package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task;

import java.io.Serializable;

import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.collections.tracked.TrackCreated;

public interface Task<T> extends Runnable, TrackCreated, Serializable {

	String getId();

	T getResult();

	Exception getException();

	String getFromMember();
}
