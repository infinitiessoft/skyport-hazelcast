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
