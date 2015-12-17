package com.infinities.skyport.distributed.impl.hazelcast;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.ISemaphore;
import com.infinities.skyport.distributed.DistributedAtomicLong;
import com.infinities.skyport.distributed.DistributedDataStructureFactory;
import com.infinities.skyport.distributed.DistributedSemaphore;

public class HazelcastDataStructureFactory implements DistributedDataStructureFactory {

	public HazelcastDataStructureFactory(HazelcastInstance hazelcastInstance) {
		this.hazelcastInstance = hazelcastInstance;
	}

	@Override
	public DistributedSemaphore getSemaphore(String name, int initPermits) {
		ISemaphore semaphore = hazelcastInstance.getSemaphore(name);
		semaphore.init(initPermits);
		return new HazelcastSemaphore(semaphore);
	}

	@Override
	public DistributedAtomicLong getAtomicLong(String name) {
		return new HazelcastAtomicLong(hazelcastInstance.getAtomicLong(name));
	}

	@Override
	public Lock getLock(String name) {
		return hazelcastInstance.getLock(name);
	}

	@Override
	public Condition getCondition(Lock lock, String conditionName) {
		return ((ILock) lock).newCondition(conditionName);
	}


	protected final HazelcastInstance hazelcastInstance;


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hazelcastInstance == null) ? 0 : hazelcastInstance.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HazelcastDataStructureFactory other = (HazelcastDataStructureFactory) obj;
		if (hazelcastInstance == null) {
			if (other.hazelcastInstance != null)
				return false;
		} else if (!hazelcastInstance.equals(other.hazelcastInstance))
			return false;
		return true;
	}

}
