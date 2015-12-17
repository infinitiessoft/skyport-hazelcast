package com.infinities.skyport.distributed.impl.hazelcast;

import java.util.concurrent.ScheduledExecutorService;

import com.hazelcast.core.HazelcastInstance;
import com.infinities.skyport.ServiceProvider;
import com.infinities.skyport.distributed.DistributedExecutor;
import com.infinities.skyport.distributed.DistributedThreadPool;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.Hazeltask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.HazeltaskInstance;
import com.infinities.skyport.exception.SkyportException;
import com.infinities.skyport.model.PoolConfig;
import com.infinities.skyport.model.PoolSize;

public class HazelcastThreadPool implements DistributedThreadPool {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private HazeltaskInstance longInstance, midInstance, shortInstance;
	private DistributedExecutor longThreadPool;
	private DistributedExecutor mediumThreadPool;
	private DistributedExecutor shortThreadPool;


	public HazelcastThreadPool() {

	}

	public HazelcastThreadPool(String name, HazelcastInstance hazelcastInstance, PoolConfig longPoolConfig,
			PoolConfig mediumPoolConfig, PoolConfig shortPoolConfig, ScheduledExecutorService scheduler,
			ServiceProvider serviceProvider) {
		// TaskMapStore longStore = new TaskMapStore(name + "_" +
		// PoolSize.LONG.name(), taskStore);
		longInstance = Hazeltask.newHazeltaskInstance(name + "_" + PoolSize.LONG.name(), hazelcastInstance, longPoolConfig,
				scheduler, serviceProvider);
		longThreadPool = longInstance.getExecutorService();

		// TaskMapStore midStore = new TaskMapStore(name + "_" +
		// PoolSize.MEDIUM.name(), taskStore);
		midInstance = Hazeltask.newHazeltaskInstance(name + "_" + PoolSize.MEDIUM.name(), hazelcastInstance, longPoolConfig,
				scheduler, serviceProvider);
		mediumThreadPool = midInstance.getExecutorService();

		// TaskMapStore shortStore = new TaskMapStore(name + "_" +
		// PoolSize.SHORT.name(), taskStore);
		shortInstance = Hazeltask.newHazeltaskInstance(name + "_" + PoolSize.SHORT.name(), hazelcastInstance,
				longPoolConfig, scheduler, serviceProvider);
		shortThreadPool = shortInstance.getExecutorService();
	}

	@Override
	public DistributedExecutor getThreadPool(PoolSize poolSize) {
		switch (poolSize) {
		case SHORT:
			return shortThreadPool;
		case MEDIUM:
			return mediumThreadPool;
		case LONG:
			return longThreadPool;
		default:
			throw new SkyportException("No thread pool with size: " + poolSize);
		}
	}

	@Override
	public void shutdown() {
		if (shortInstance != null) {
			shortInstance.shutdown();
		}
		if (midInstance != null) {
			midInstance.shutdown();
		}
		if (longInstance != null) {
			longInstance.shutdown();
		}
	}

	@Override
	public void destroy() {
		shutdown();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((longInstance == null) ? 0 : longInstance.hashCode());
		result = prime * result + ((longThreadPool == null) ? 0 : longThreadPool.hashCode());
		result = prime * result + ((mediumThreadPool == null) ? 0 : mediumThreadPool.hashCode());
		result = prime * result + ((midInstance == null) ? 0 : midInstance.hashCode());
		result = prime * result + ((shortInstance == null) ? 0 : shortInstance.hashCode());
		result = prime * result + ((shortThreadPool == null) ? 0 : shortThreadPool.hashCode());
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
		HazelcastThreadPool other = (HazelcastThreadPool) obj;
		if (longInstance == null) {
			if (other.longInstance != null)
				return false;
		} else if (!longInstance.equals(other.longInstance))
			return false;
		if (longThreadPool == null) {
			if (other.longThreadPool != null)
				return false;
		} else if (!longThreadPool.equals(other.longThreadPool))
			return false;
		if (mediumThreadPool == null) {
			if (other.mediumThreadPool != null)
				return false;
		} else if (!mediumThreadPool.equals(other.mediumThreadPool))
			return false;
		if (midInstance == null) {
			if (other.midInstance != null)
				return false;
		} else if (!midInstance.equals(other.midInstance))
			return false;
		if (shortInstance == null) {
			if (other.shortInstance != null)
				return false;
		} else if (!shortInstance.equals(other.shortInstance))
			return false;
		if (shortThreadPool == null) {
			if (other.shortThreadPool != null)
				return false;
		} else if (!shortThreadPool.equals(other.shortThreadPool))
			return false;
		return true;
	}

}
