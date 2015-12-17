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

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.hazelcast.core.ICondition;
import com.infinities.skyport.distributed.DistributedCondition;

public class HazelcastCondition implements DistributedCondition {

	private ICondition icondition;


	public HazelcastCondition(ICondition icondition) {
		this.icondition = icondition;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void await() throws InterruptedException {
		icondition.await();
	}

	@Override
	public void awaitUninterruptibly() {
		icondition.awaitUninterruptibly();
	}

	@Override
	public long awaitNanos(long nanosTimeout) throws InterruptedException {
		return icondition.awaitNanos(nanosTimeout);
	}

	@Override
	public boolean await(long time, TimeUnit unit) throws InterruptedException {
		return icondition.await(time, unit);
	}

	@Override
	public boolean awaitUntil(Date deadline) throws InterruptedException {
		return icondition.awaitUntil(deadline);
	}

	@Override
	public void signal() {
		icondition.signal();
	}

	@Override
	public void signalAll() {
		icondition.signalAll();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((icondition == null) ? 0 : icondition.hashCode());
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
		HazelcastCondition other = (HazelcastCondition) obj;
		if (icondition == null) {
			if (other.icondition != null)
				return false;
		} else if (!icondition.equals(other.icondition))
			return false;
		return true;
	}

}
