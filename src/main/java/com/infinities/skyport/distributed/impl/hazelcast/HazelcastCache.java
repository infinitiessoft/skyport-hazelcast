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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IMap;
import com.infinities.skyport.distributed.DistributedCache;

public class HazelcastCache<K, V> implements DistributedCache<K, V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	// private static final Logger logger =
	// LoggerFactory.getLogger(HazelcastCache.class);
	private volatile IMap<K, V> map;
	private volatile IAtomicReference<String> e;
	private volatile IAtomicLong isInitialized;


	public HazelcastCache(String name, Map<K, V> map, HazelcastInstance hazelcastInstance) {
		if (map == null) {
			throw new NullPointerException("map cannot be null");
		}
		this.map = hazelcastInstance.getMap(name);
		this.e = hazelcastInstance.getAtomicReference(name + "Throwable");
		this.isInitialized = hazelcastInstance.getAtomicLong(name + "Initialized");
		isInitialized.set(1L);
		reload(map);
	}

	public HazelcastCache(String name, Throwable e, HazelcastInstance hazelcastInstance) {
		if (e == null) {
			throw new NullPointerException("e cannot be null");
		}
		this.map = hazelcastInstance.getMap(name);
		this.e = hazelcastInstance.getAtomicReference(name + "Throwable");
		this.e.set(e.getMessage());
		this.isInitialized = hazelcastInstance.getAtomicLong(name + "Initialized");
		isInitialized.set(1L);
	}

	public HazelcastCache(String name, HazelcastInstance hazelcastInstance) {
		this.map = hazelcastInstance.getMap(name);
		this.e = hazelcastInstance.getAtomicReference(name + "Throwable");
		this.isInitialized = hazelcastInstance.getAtomicLong(name + "Initialized");
		if (isInitialized.get() == 0L) {
			this.e.set("cache not initialized yet");
		}
	}

	@Override
	public void reload(Map<K, V> map) {
		this.map.clear();
		this.map.putAll(map);
		e.set(null);
	}

	@Override
	public void reload(Throwable e) {
		this.e.set(e.getMessage());
		this.map.clear();
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		checkMessage();
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		checkMessage();
		return map.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		checkMessage();
		return map.entrySet();
	}

	@Override
	public V get(Object key) {
		checkMessage();
		return map.get(key);
	}

	@Override
	public boolean isEmpty() {
		checkMessage();
		return map.isEmpty();
	}

	@Override
	public Set<K> keySet() {
		checkMessage();
		return map.keySet();
	}

	@Override
	public V put(K key, V value) {
		checkMessage();
		return map.put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		if (m == null) {
			throw new NullPointerException("map cannot be null");
		}
		map.putAll(m);
	}

	@Override
	public V remove(Object key) {
		checkMessage();
		return map.remove(key);
	}

	@Override
	public int size() {
		checkMessage();
		return map.size();
	}

	@Override
	public Collection<V> values() {
		checkMessage();
		return map.values();
	}

//	@Override
//	public boolean tryLock(K key) {
//		try {
//			return map.tryLock(key, 3, TimeUnit.SECONDS);
//		} catch (InterruptedException e) {
//			return false;
//		}
//	}
//
//	@Override
//	public void unlock(K key) {
//		map.unlock(key);
//	}

	@Override
	public void destroy() {
		isInitialized.destroy();
		map.destroy();
		e.destroy();
	}

	@Override
	public void set(K key, V value) {
		map.set(key, value);
	}

	private void checkMessage() {
		if (isInitialized.get() == 0L) {
			throw new IllegalStateException("cache not initialized yet");
		}
		String message = e.get();
		if (!Strings.isNullOrEmpty(message)) {
			throw new IllegalStateException(message);
		}
	}

//	@Override
//	public void lock(K key) {
//		map.lock(key);
//	}

	@Override
	public void delete(K key) {
		map.delete(key);
	}

	@Override
	public void refresh() {
		e.set(null);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((e == null) ? 0 : e.hashCode());
		result = prime * result + ((isInitialized == null) ? 0 : isInitialized.hashCode());
		result = prime * result + ((map == null) ? 0 : map.hashCode());
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
		HazelcastCache other = (HazelcastCache) obj;
		if (e == null) {
			if (other.e != null)
				return false;
		} else if (!e.equals(other.e))
			return false;
		if (isInitialized == null) {
			if (other.isInitialized != null)
				return false;
		} else if (!isInitialized.equals(other.isInitialized))
			return false;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		return true;
	}

}
