package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.collections.tracked;

import java.util.concurrent.BlockingQueue;

public interface ITrackedQueue<E> extends BlockingQueue<E> {

	Long getOldestItemTime();

	Long getLastAddedTime();

	Long getLastRemovedTime();
}
