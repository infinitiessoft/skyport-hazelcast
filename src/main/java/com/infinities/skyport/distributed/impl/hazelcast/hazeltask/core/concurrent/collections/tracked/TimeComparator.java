package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.collections.tracked;

import java.util.Comparator;

public class TimeComparator<E extends TrackCreated> implements Comparator<E> {

	@Override
	public int compare(E o1, E o2) {
		Long t1 = o1.getTimeCreated();
		Long t2 = o2.getTimeCreated();
		return t1.compareTo(t2);
	}

}
