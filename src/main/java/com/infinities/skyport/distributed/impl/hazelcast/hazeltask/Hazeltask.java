package com.infinities.skyport.distributed.impl.hazelcast.hazeltask;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Strings;
import com.hazelcast.core.HazelcastInstance;
import com.infinities.skyport.ServiceProvider;
import com.infinities.skyport.model.PoolConfig;

public class Hazeltask {

	public static final ConcurrentMap<String, HazeltaskInstance> instances = new ConcurrentHashMap<String, HazeltaskInstance>();


	private Hazeltask() {

	}

	public static HazeltaskInstance getInstanceByName(String topology) {
		return instances.get(topology);
	}

	public synchronized static HazeltaskInstance newHazeltaskInstance(String topologyName, HazelcastInstance hazelcast,
			PoolConfig poolConfig, ScheduledExecutorService scheduler, ServiceProvider serviceProvider) {
		if (Strings.isNullOrEmpty(topologyName)) {
			throw new NullPointerException(topologyName);
		}
		HazeltaskInstance instance = instances.get(topologyName);
		if (instance == null) {
			checkNotNull(hazelcast, "invalid hazelcast");
			checkNotNull(poolConfig, "invalid poolConfig");
			checkNotNull(scheduler, "invalid scheduler");
			checkNotNull(serviceProvider, "invalid service provider");
			instance = new HazeltaskInstance(topologyName, hazelcast, poolConfig, scheduler, serviceProvider);
			if (instances.putIfAbsent(instance.getTopologyName(), instance) != null) {
				throw new IllegalStateException("An instance for the name " + topologyName + " already exists!");
			}
			instance.start();
		}

		return instance;
	}

}
