/*******************************************************************************
 * Copyright 2015 InfinitiesSoft Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
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
