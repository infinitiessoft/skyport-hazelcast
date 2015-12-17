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

import com.hazelcast.core.IAtomicLong;
import com.infinities.skyport.distributed.DistributedAtomicLong;

/**
 * Hazelcast implementation of a distributed atomic long. Delegates to an
 * {@link com.hazelcast.core.IAtomicLong}.
 * 
 * @author vanessa.williams
 */
public class HazelcastAtomicLong implements DistributedAtomicLong {

	private final static long serialVersionUID = 1L;
	
	private final IAtomicLong delegate;


	public HazelcastAtomicLong(IAtomicLong delegate) {
		this.delegate = delegate;
	}

	@Override
	public String getName() {
		return delegate.getName();
	}

	@Override
	public long addAndGet(long l) {
		return delegate.addAndGet(l);
	}

	@Override
	public boolean compareAndSet(long l, long l2) {
		return delegate.compareAndSet(l, l2);
	}

	@Override
	public long decrementAndGet() {
		return delegate.decrementAndGet();
	}

	@Override
	public long get() {
		return delegate.get();
	}

	@Override
	public long getAndAdd(long l) {
		return delegate.getAndAdd(l);
	}

	@Override
	public long getAndSet(long l) {
		return delegate.getAndSet(l);
	}

	@Override
	public long incrementAndGet() {
		return delegate.incrementAndGet();
	}

	@Override
	public long getAndIncrement() {
		return delegate.getAndIncrement();
	}

	@Override
	public void set(long l) {
		delegate.set(l);
	}

	@Override
	public void destroy() {
		delegate.destroy();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((delegate == null) ? 0 : delegate.hashCode());
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
		HazelcastAtomicLong other = (HazelcastAtomicLong) obj;
		if (delegate == null) {
			if (other.delegate != null)
				return false;
		} else if (!delegate.equals(other.delegate))
			return false;
		return true;
	}

}
