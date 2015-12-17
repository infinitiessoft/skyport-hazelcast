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

import com.hazelcast.core.IAtomicReference;
import com.infinities.skyport.distributed.DistributedAtomicReference;
import com.infinities.skyport.distributed.DistributedFunction;

public class HazelcastAtomicReference<K> implements DistributedAtomicReference<K> {

	
	private IAtomicReference<K> reference;


	public HazelcastAtomicReference(IAtomicReference<K> reference) {
		this.reference = reference;
	}

	@Override
	public boolean compareAndSet(K expect, K update) {
		return reference.compareAndSet(expect, update);
	}

	@Override
	public K get() {
		return reference.get();
	}

	@Override
	public void set(K newValue) {
		reference.set(newValue);
	}

	@Override
	public K getAndSet(K newValue) {
		return reference.getAndSet(newValue);
	}

	@Override
	public K setAndGet(K update) {
		return reference.setAndGet(update);
	}

	@Override
	public boolean isNull() {
		return reference.isNull();
	}

	@Override
	public void clear() {
		reference.clear();
	}

	@Override
	public boolean contains(K value) {
		return reference.contains(value);
	}

	@Override
	public void destroy() {
		reference.destroy();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((reference == null) ? 0 : reference.hashCode());
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
		@SuppressWarnings("rawtypes")
		HazelcastAtomicReference other = (HazelcastAtomicReference) obj;
		if (reference == null) {
			if (other.reference != null)
				return false;
		} else if (!reference.equals(other.reference))
			return false;
		return true;
	}

	@Override
	public void alter(final DistributedFunction<K, K> function) {
		reference.alter(new HazelcastFunction<K>(function));
	}

}
