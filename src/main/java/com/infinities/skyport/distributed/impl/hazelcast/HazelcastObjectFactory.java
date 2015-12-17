package com.infinities.skyport.distributed.impl.hazelcast;

import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.infinities.skyport.ServiceProvider;
import com.infinities.skyport.distributed.DistributedAtomicLong;
import com.infinities.skyport.distributed.DistributedAtomicReference;
import com.infinities.skyport.distributed.DistributedCache;
import com.infinities.skyport.distributed.DistributedLock;
import com.infinities.skyport.distributed.DistributedMap;
import com.infinities.skyport.distributed.DistributedObjectFactory;
import com.infinities.skyport.distributed.DistributedThreadPool;
import com.infinities.skyport.distributed.impl.hazelcast.shiro.cache.HazelcastCacheManager;
import com.infinities.skyport.distributed.shiro.DistributedCacheManager;
import com.infinities.skyport.model.PoolConfig;

public class HazelcastObjectFactory implements DistributedObjectFactory {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final HazelcastInstance hazelcastInstance;
	private final String name;
	private final String group;


	public HazelcastObjectFactory(String distributedKey) throws FileNotFoundException {
		this.hazelcastInstance = HazelcastHelper.getHazelcastInstance(distributedKey);
		this.name = distributedKey;
		this.group = HazelcastHelper.getGroup();
	}

	public HazelcastObjectFactory(HazelcastInstance hazelcastInstance) throws FileNotFoundException {
		this.hazelcastInstance = hazelcastInstance;
		name = hazelcastInstance.getName();
		this.group = HazelcastHelper.getGroup();
	}

	@Override
	public <K, V> DistributedMap<K, V> getMap(String name) {
		IMap<K, V> imap = hazelcastInstance.getMap(name);
		return new HazelcastMap<K, V>(imap);
	}

	@Override
	public <K, V> DistributedCache<K, V> getCache(String name, Throwable e) {
		return new HazelcastCache<K, V>(name, e, hazelcastInstance);
	}

	@Override
	public <K, V> DistributedCache<K, V> getCache(String name, Map<K, V> map) {
		return new HazelcastCache<K, V>(name, map, hazelcastInstance);
	}

	@Override
	public DistributedAtomicLong getAtomicLong(String name) {
		return new HazelcastAtomicLong(hazelcastInstance.getAtomicLong(name));
	}

	@Override
	public DistributedThreadPool getThreadPool(String name, PoolConfig longPoolConfig, PoolConfig mediumPoolConfig,
			PoolConfig shortPoolConfig, ScheduledExecutorService scheduler, ServiceProvider serviceProvider) {
		return new HazelcastThreadPool(name, hazelcastInstance, longPoolConfig, mediumPoolConfig, shortPoolConfig,
				scheduler, serviceProvider);
	}

	@Override
	public <K> DistributedAtomicReference<K> getAtomicReference(String name) {
		IAtomicReference<K> reference = hazelcastInstance.getAtomicReference(name);
		return new HazelcastAtomicReference<K>(reference);
	}

	@Override
	public void close() {
		if (hazelcastInstance == null || !hazelcastInstance.getLifecycleService().isRunning()) {
			return;
		}
		hazelcastInstance.getLifecycleService().shutdown();
		HazelcastHelper.instances.remove(name);
	}

	@Override
	public DistributedLock getLock(String name) {
		return new HazelcastLock(hazelcastInstance.getLock(name));
	}

	@Override
	public <K, V> DistributedCache<K, V> getCache(String name) {
		return new HazelcastCache<K, V>(name, hazelcastInstance);
	}

	@Override
	public DistributedCacheManager getDistributedCacheManager() {
		return new HazelcastCacheManager();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((group == null) ? 0 : group.hashCode());
		result = prime * result + ((hazelcastInstance == null) ? 0 : hazelcastInstance.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		HazelcastObjectFactory other = (HazelcastObjectFactory) obj;
		if (group == null) {
			if (other.group != null)
				return false;
		} else if (!group.equals(other.group))
			return false;
		if (hazelcastInstance == null) {
			if (other.hazelcastInstance != null)
				return false;
		} else if (!hazelcastInstance.equals(other.hazelcastInstance))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	@Override
	public String getGroup() {
		return group;
	}

	@Override
	public Set<com.infinities.skyport.model.Member> getMembers() {
		Set<com.infinities.skyport.model.Member> rets = new HashSet<com.infinities.skyport.model.Member>();
		for (Member member : hazelcastInstance.getCluster().getMembers()) {
			com.infinities.skyport.model.Member m = new com.infinities.skyport.model.Member();
			m.setHostName(member.getSocketAddress().getHostName());
			m.setPort(member.getSocketAddress().getPort());
			rets.add(m);
		}
		return rets;
	}

}
