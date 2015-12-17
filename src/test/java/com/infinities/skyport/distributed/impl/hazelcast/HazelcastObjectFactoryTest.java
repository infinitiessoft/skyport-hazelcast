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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.HazelcastInstance;
import com.infinities.skyport.ServiceProvider;
import com.infinities.skyport.distributed.DistributedAtomicLong;
import com.infinities.skyport.distributed.DistributedAtomicReference;
import com.infinities.skyport.distributed.DistributedCache;
import com.infinities.skyport.distributed.DistributedLock;
import com.infinities.skyport.distributed.DistributedMap;
import com.infinities.skyport.distributed.DistributedThreadPool;
import com.infinities.skyport.model.PoolConfig;
import com.infinities.skyport.model.PoolSize;

public class HazelcastObjectFactoryTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private HazelcastObjectFactory factory;
	private ScheduledExecutorService executorService;
	private ServiceProvider serviceProvider;
	private ScheduledFuture<?> future;


	@Before
	public void setUp() throws Exception {
		future = context.mock(ScheduledFuture.class);
		factory = new HazelcastObjectFactory("factorytest");
		executorService = context.mock(ScheduledExecutorService.class);
		serviceProvider = context.mock(ServiceProvider.class);
	}

	@After
	public void tearDown() throws Exception {
		factory.close();
	}

	@Test
	public void testHazelcastObjectFactoryString() throws FileNotFoundException {
		HazelcastObjectFactory factory2 = new HazelcastObjectFactory("factorytest");
		assertEquals(factory2, factory);
		factory2.close();
	}

	@Test
	public void testHazelcastObjectFactoryHazelcastInstance() throws FileNotFoundException {
		HazelcastInstance instance = HazelcastHelper.getHazelcastInstance("factorytest");
		HazelcastObjectFactory factory2 = new HazelcastObjectFactory(instance);
		assertEquals(factory2, factory);
		factory2.close();
	}

	@Test
	public void testGetMap() {
		DistributedMap<String, String> map = factory.getMap("factorytest");
		DistributedMap<String, String> map2 = factory.getMap("factorytest");
		assertEquals(map, map2);
		DistributedMap<String, String> map3 = factory.getMap("factorytest3");
		assertNotEquals(map, map3);
	}

	@Test
	public void testGetCacheStringThrowable() {
		Map<String, String> map = new HashMap<String, String>();
		DistributedCache<String, String> cache = factory.getCache("factorytest", map);
		DistributedCache<String, String> cache2 = factory.getCache("factorytest", map);
		assertEquals(cache, cache2);
		DistributedCache<String, String> cache3 = factory.getCache("factorytest3", map);
		assertNotEquals(cache, cache3);
		cache.get("test");
	}

	@Test
	public void testGetCacheStringMapOfKV() {
		Throwable e = new IllegalStateException("factorytest");
		DistributedCache<String, String> cache = factory.getCache("factorytest", e);
		DistributedCache<String, String> cache2 = factory.getCache("factorytest", e);
		assertEquals(cache, cache2);
		DistributedCache<String, String> cache3 = factory.getCache("factorytest3", e);
		assertNotEquals(cache, cache3);
	}

	@Test(expected = IllegalStateException.class)
	public void testGetCacheStringThrowable2() {
		Throwable e = new IllegalStateException("test");
		DistributedCache<String, String> cache = factory.getCache("factorytest", e);
		DistributedCache<String, String> cache2 = factory.getCache("factorytest", e);
		assertEquals(cache, cache2);
		DistributedCache<String, String> cache3 = factory.getCache("factorytest3", e);
		assertNotEquals(cache, cache3);
		cache.get("test");
	}

	@Test
	public void testGetAtomicLong() {
		DistributedAtomicLong atomicLong = factory.getAtomicLong("factorytest");
		DistributedAtomicLong atomicLong2 = factory.getAtomicLong("factorytest");
		assertEquals(atomicLong, atomicLong2);
		DistributedAtomicLong atomicLong3 = factory.getAtomicLong("factorytest3");
		assertNotEquals(atomicLong, atomicLong3);
	}

	// @Test
	// public void testGetReentrantReadWriteLock() {
	// DistributedReadWriteLock lock =
	// factory.getReentrantReadWriteLock("factorytest");
	// DistributedReadWriteLock lock2 =
	// factory.getReentrantReadWriteLock("factorytest");
	// assertEquals(lock, lock2);
	// DistributedReadWriteLock lock3 =
	// factory.getReentrantReadWriteLock("factorytest3");
	// assertNotEquals(lock, lock3);
	// }

	@Test
	public void testGetThreadPool() {
		context.checking(new Expectations() {

			{
				exactly(6).of(executorService).schedule(with(any(Runnable.class)), with(any(Long.class)),
						with(any(TimeUnit.class)));
				will(returnValue(future));

			}
		});
		DistributedThreadPool pool = factory.getThreadPool("test", new PoolConfig(PoolSize.LONG), new PoolConfig(
				PoolSize.MEDIUM), new PoolConfig(PoolSize.SHORT), executorService, serviceProvider);
		DistributedThreadPool pool2 = factory.getThreadPool("test", new PoolConfig(PoolSize.LONG), new PoolConfig(
				PoolSize.MEDIUM), new PoolConfig(PoolSize.SHORT), executorService, serviceProvider);
		assertEquals(pool, pool2);
		DistributedThreadPool pool3 = factory.getThreadPool("test3", new PoolConfig(PoolSize.LONG), new PoolConfig(
				PoolSize.MEDIUM), new PoolConfig(PoolSize.SHORT), executorService, serviceProvider);
		assertNotEquals(pool, pool3);
	}

	@Test
	public void testGetAtomicReference() {
		DistributedAtomicReference<String> ref = factory.getAtomicReference("factorytest");
		DistributedAtomicReference<String> ref2 = factory.getAtomicReference("factorytest");
		assertEquals(ref, ref2);
		DistributedAtomicReference<String> ref3 = factory.getAtomicReference("factorytest3");
		assertNotEquals(ref, ref3);
	}

	@Test
	public void testClose() {
		// HazelcastHelper.instances.put("test", factory);
		// assertEquals(1, HazelcastHelper.instances.size());
		factory.close();
		// assertEquals(0, HazelcastHelper.instances.size());
	}

	@Test
	public void testGetLock() {
		DistributedLock lock = factory.getLock("factorytest");
		DistributedLock lock2 = factory.getLock("factorytest");
		assertEquals(lock, lock2);
		DistributedLock lock3 = factory.getLock("factorytest3");
		assertNotEquals(lock, lock3);
	}

	@Test
	public void testGetCacheString() {
		DistributedCache<String, String> cache = factory.getCache("factorytest");
		DistributedCache<String, String> cache2 = factory.getCache("factorytest");
		assertEquals(cache, cache2);
		DistributedCache<String, String> cache3 = factory.getCache("factorytest3");
		assertNotEquals(cache, cache3);
	}

}
