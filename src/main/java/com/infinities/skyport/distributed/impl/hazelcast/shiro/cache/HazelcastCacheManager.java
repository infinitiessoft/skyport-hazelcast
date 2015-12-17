/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright 2, 2015nership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.infinities.skyport.distributed.impl.hazelcast.shiro.cache;

import java.util.Map;

import org.apache.shiro.ShiroException;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.cache.MapCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.infinities.skyport.distributed.impl.hazelcast.HazelcastHelper;
import com.infinities.skyport.distributed.shiro.DistributedCacheManager;

public class HazelcastCacheManager implements DistributedCacheManager {

	public static final Logger log = LoggerFactory.getLogger(HazelcastCacheManager.class);

	private boolean implicitlyCreated;
	private HazelcastInstance hazelcastInstance;


	/**
	 * Returns a {@link MapCache} instance representing the named
	 * Hazelcast-managed {@link com.hazelcast.core.IMap IMap}. The Hazelcast Map
	 * is obtained by calling {@link HazelcastInstance#getMap(String)
	 * hazelcastInstance.getMap(name)}.
	 * 
	 * @param name
	 *            the name of the cache to acquire.
	 * @param <K>
	 *            the type of map key
	 * @param <V>
	 *            the type of map value
	 * @return a {@link MapCache} instance representing the named
	 *         Hazelcast-managed {@link com.hazelcast.core.IMap IMap}.
	 * @throws CacheException
	 * @see HazelcastInstance#getMap(String)
	 * @see #ensureHazelcastInstance()
	 * 
	 */
	@Override
	public <K, V> Cache<K, V> getCache(String name) throws CacheException {
		Map<K, V> map = ensureHazelcastInstance().getMap(name); // returned map
																// is a
																// ConcurrentMap
		return new MapCache<K, V>(name, map);
	}

	/**
	 * Ensures that this implementation has a backing {@link HazelcastInstance},
	 * and if not, implicitly creates one via {@link #createHazelcastInstance()}
	 * .
	 * 
	 * @return the backing (potentially newly created) {@code HazelcastInstance}
	 *         .
	 * @see #createHazelcastInstance()
	 * @see HazelcastInstance
	 */
	protected HazelcastInstance ensureHazelcastInstance() {
		if (this.hazelcastInstance == null) {
			hazelcastInstance = HazelcastHelper.getDefaultInstance();
			this.implicitlyCreated = true;
		}
		return this.hazelcastInstance;
	}

	/**
	 * Initializes this instance by {@link #ensureHazelcastInstance() ensuring}
	 * there is a backing {@link HazelcastInstance}.
	 * 
	 * @throws ShiroException
	 * @see #ensureHazelcastInstance()
	 * @see HazelcastInstance
	 */
	@Override
	public void init() throws ShiroException {
		ensureHazelcastInstance();
	}

	// needed for unit tests only - not part of Shiro's public API

	/**
	 * NOT PART OF SHIRO'S ACCESSIBLE API. DO NOT DEPEND ON THIS. This method
	 * was added for testing purposes only.
	 * <p/>
	 * Returns {@code true} if this {@code HazelcastCacheManager} instance
	 * implicitly created the backing {@code HazelcastInstance}, or
	 * {@code false} if one was externally provided via
	 * {@link #setHazelcastInstance(com.hazelcast.core.HazelcastInstance)
	 * setHazelcastInstance}.
	 * 
	 * @return {@code true} if this {@code HazelcastCacheManager} instance
	 *         implicitly created the backing {@code HazelcastInstance}, or
	 *         {@code false} if one was externally provided via
	 *         {@link #setHazelcastInstance(com.hazelcast.core.HazelcastInstance)
	 *         setHazelcastInstance}.
	 */
	protected final boolean isImplicitlyCreated() {
		return this.implicitlyCreated;
	}

	/**
	 * Destroys any {@link #ensureHazelcastInstance() implicitly created}
	 * backing {@code HazelcastInstance}. If the backing Hazelcast was not
	 * implicitly created (i.e. because it was configured externally and
	 * supplied via
	 * {@link #setHazelcastInstance(com.hazelcast.core.HazelcastInstance)
	 * setHazelcastInstance}), this method does nothing.
	 * 
	 * @throws Exception
	 *             if there is a problem shutting down
	 */
	@Override
	public void destroy() throws Exception {
		if (this.implicitlyCreated) {
			try {
				this.hazelcastInstance.getLifecycleService().shutdown();
			} catch (Throwable t) {
				if (log.isWarnEnabled()) {
					log.warn("Unable to cleanly shutdown implicitly created HazelcastInstance.  "
							+ "Ignoring (shutting down)...", t);
				}
			} finally {
				this.hazelcastInstance = null;
				this.implicitlyCreated = false;
			}
		}
	}

	/**
	 * Returns the {@code HazelcastInstance} from which named
	 * {@link java.util.concurrent.ConcurrentMap ConcurrentMap} instances will
	 * be acquired to create {@link MapCache} instances.
	 * 
	 * @return the {@code HazelcastInstance} from which named
	 *         {@link java.util.concurrent.ConcurrentMap ConcurrentMap}
	 *         instances will be acquired to create {@link MapCache} instances.
	 */
	public HazelcastInstance getHazelcastInstance() {
		return hazelcastInstance;
	}

	/**
	 * Sets the {@code HazelcastInstance} from which named
	 * {@link java.util.concurrent.ConcurrentMap ConcurrentMap} instances will
	 * be acquired to create {@link MapCache} instances.
	 * 
	 * @param hazelcastInstance
	 *            the {@code HazelcastInstance} from which named
	 *            {@link java.util.concurrent.ConcurrentMap ConcurrentMap}
	 *            instances will be acquired to create {@link MapCache}
	 *            instances.
	 */
	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.hazelcastInstance = hazelcastInstance;
	}

	public void setCacheManagerConfigFile(String classpathLocation) {
	}

	@Override
	public boolean getSchedulerEnabled() {
		return false;
	}
}
