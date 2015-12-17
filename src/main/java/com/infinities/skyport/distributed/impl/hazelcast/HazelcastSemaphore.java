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

import com.hazelcast.core.ISemaphore;
import com.infinities.skyport.distributed.DistributedSemaphore;

/**
 * Hazelcast implementation of a distributed semaphore. Delegates to an
 * {@link com.hazelcast.core.ISemaphore}.
 * 
 * @author vanessa.williams
 */
public class HazelcastSemaphore implements DistributedSemaphore {

	private final ISemaphore delegate;


	public String getName() {
		return delegate.getName();
	}

	public HazelcastSemaphore(ISemaphore delegate) {
		this.delegate = delegate;
	}

	@Override
	public void acquire() throws InterruptedException {
		delegate.acquire();
	}

	@Override
	public boolean tryAcquire() {
		return delegate.tryAcquire();
	}

	@Override
	public boolean tryAcquire(long l, TimeUnit timeUnit) throws InterruptedException {
		return delegate.tryAcquire(l, timeUnit);
	}

	@Override
	public void release() {
		delegate.release();
	}

	@Override
	public int availablePermits() {
		return delegate.availablePermits();
	}
}
