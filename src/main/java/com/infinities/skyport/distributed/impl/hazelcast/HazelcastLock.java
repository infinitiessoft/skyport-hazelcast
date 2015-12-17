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

import java.util.concurrent.TimeUnit;

import com.hazelcast.core.ILock;
import com.infinities.skyport.distributed.DistributedCondition;
import com.infinities.skyport.distributed.DistributedLock;

public class HazelcastLock implements DistributedLock {

	private ILock ilock;


	public HazelcastLock(ILock ilock) {
		this.ilock = ilock;
	}

	@Override
	public void destroy() {
		ilock.destroy();
	}

	@Override
	public void lock() {
		ilock.lock();
	}

	@Override
	public boolean tryLock() {
		return ilock.tryLock();
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		return ilock.tryLock(time, unit);
	}

	@Override
	public void unlock() {
		ilock.unlock();
	}

	@Override
	public void lock(long leaseTime, TimeUnit timeUnit) {
		ilock.lock(leaseTime, timeUnit);
	}

	@Override
	public void forceUnlock() {
		ilock.forceUnlock();
	}

	@Override
	public DistributedCondition newCondition(String name) {
		return new HazelcastCondition(ilock.newCondition(name));
	}

	@Override
	public boolean isLocked() {
		return ilock.isLocked();
	}

	@Override
	public boolean isLockedByCurrentThread() {
		return ilock.isLockedByCurrentThread();
	}

	@Override
	public int getLockCount() {
		return ilock.getLockCount();
	}

	@Override
	public long getRemainingLeaseTime() {
		return ilock.getRemainingLeaseTime();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ilock == null) ? 0 : ilock.hashCode());
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
		HazelcastLock other = (HazelcastLock) obj;
		if (ilock == null) {
			if (other.ilock != null)
				return false;
		} else if (!ilock.equals(other.ilock))
			return false;
		return true;
	}

}
