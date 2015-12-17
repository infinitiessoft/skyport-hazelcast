package com.infinities.skyport.distributed.impl.hazelcast;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.hazelcast.core.IMap;
import com.infinities.skyport.distributed.DistributedMap;

public class HazelcastMap<K, V> implements DistributedMap<K, V> {

	private IMap<K, V> imap;


	public HazelcastMap(IMap<K, V> imap) {
		this.imap = imap;
	}

	@Override
	public void clear() {
		imap.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return imap.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return imap.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return imap.entrySet();
	}

	@Override
	public V get(Object key) {
		return imap.get(key);
	}

	@Override
	public boolean isEmpty() {
		return imap.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		return imap.keySet();
	}

	@Override
	public V put(K key, V value) {
		return imap.put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		imap.putAll(m);
	}

	@Override
	public V remove(Object key) {
		return imap.remove(key);
	}

	@Override
	public int size() {
		return imap.size();
	}

	@Override
	public Collection<V> values() {
		return imap.values();
	}

	@Override
	public void destroy() {
		imap.destroy();
	}

	@Override
	public void lock(K key) {
		imap.lock(key);
	}

	@Override
	public void unlock(K key) {
		imap.unlock(key);
	}

	@Override
	public void set(K key, V value) {
		imap.set(key, value);
	}

	@Override
	public boolean tryLock(K key) {
		return imap.tryLock(key);
	}

	public Set<K> localKeySet() {
		return imap.localKeySet();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((imap == null) ? 0 : imap.hashCode());
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
		HazelcastMap other = (HazelcastMap) obj;
		if (imap == null) {
			if (other.imap != null)
				return false;
		} else if (!imap.equals(other.imap))
			return false;
		return true;
	}

}
