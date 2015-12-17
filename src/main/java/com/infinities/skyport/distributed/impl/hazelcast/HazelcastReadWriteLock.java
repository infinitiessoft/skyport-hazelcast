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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import com.infinities.skyport.distributed.DistributedReadWriteLock;

public class HazelcastReadWriteLock implements DistributedReadWriteLock {

	private ReadWriteLock readWriteLock;


	public HazelcastReadWriteLock(ReadWriteLock readWriteLock) {
		this.readWriteLock = readWriteLock;
	}

	@Override
	public Lock readLock() {
		return readWriteLock.readLock();
	}

	@Override
	public Lock writeLock() {
		return readWriteLock.writeLock();
	}

	@Override
	public void destroy() {
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((readWriteLock == null) ? 0 : readWriteLock.hashCode());
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
		HazelcastReadWriteLock other = (HazelcastReadWriteLock) obj;
		if (readWriteLock == null) {
			if (other.readWriteLock != null)
				return false;
		} else if (!readWriteLock.equals(other.readWriteLock))
			return false;
		return true;
	}

}
