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
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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

import com.hazelcast.core.Cluster;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.PartitionService;
import com.infinities.skyport.ServiceProvider;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.Hazeltask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.HazeltaskInstance;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.DistributedFutureTracker;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.model.PoolConfig;
import com.infinities.skyport.model.PoolSize;

public class HazeltaskTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};

	private HazelcastInstance hazelcast;
	private PoolConfig poolConfig;
	private ScheduledExecutorService scheduler;
	private ServiceProvider serviceProvider;
	private PartitionService partitionService;
	private Cluster cluster;
	private Member member;
	private IExecutorService executor;
	@SuppressWarnings("rawtypes")
	private IMap map;
	@SuppressWarnings("rawtypes")
	private ITopic topic;
	private LifecycleService lifecycleService;
	private Map<UUID, HazeltaskTask<?>> tasks;
	private Set<UUID> localKeySet;
	private UUID uuid;
	private HazeltaskTask<?> task;
	private ScheduledFuture<?> future;


	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		future = context.mock(ScheduledFuture.class);
		hazelcast = context.mock(HazelcastInstance.class);
		poolConfig = new PoolConfig(PoolSize.SHORT);
		scheduler = context.mock(ScheduledExecutorService.class);
		serviceProvider = context.mock(ServiceProvider.class);
		partitionService = context.mock(PartitionService.class);
		cluster = context.mock(Cluster.class);
		member = context.mock(Member.class);
		executor = context.mock(IExecutorService.class);
		map = context.mock(IMap.class);
		topic = context.mock(ITopic.class);
		lifecycleService = context.mock(LifecycleService.class);
		uuid = UUID.randomUUID();
		localKeySet = new HashSet<UUID>();
		localKeySet.add(uuid);
		task = context.mock(HazeltaskTask.class);
		tasks = new HashMap<UUID, HazeltaskTask<?>>();
		tasks.put(uuid, task);
		context.checking(new Expectations() {

			{
				oneOf(hazelcast).getPartitionService();
				will(returnValue(partitionService));

				oneOf(hazelcast).getCluster();
				will(returnValue(cluster));

				oneOf(cluster).getLocalMember();
				will(returnValue(member));

				oneOf(hazelcast).getExecutorService("test-com");
				will(returnValue(executor));

				oneOf(hazelcast).getMap("test-pending-tasks");
				will(returnValue(map));

				oneOf(hazelcast).getTopic("test-task-response");
				will(returnValue(topic));

				oneOf(partitionService).addMigrationListener(with(any(MigrationListener.class)));
				will(returnValue("1"));

				oneOf(topic).addMessageListener(with(any(DistributedFutureTracker.class)));
				will(returnValue("2"));

				oneOf(map).addLocalEntryListener(with(any(EntryListener.class)));
				will(returnValue("3"));

				oneOf(hazelcast).getLifecycleService();
				will(returnValue(lifecycleService));

				oneOf(lifecycleService).addLifecycleListener(with(any(LifecycleListener.class)));
				will(returnValue("4"));

				oneOf(lifecycleService).isRunning();
				will(returnValue(true));

				oneOf(map).localKeySet();
				will(returnValue(localKeySet));

				oneOf(map).getAll(localKeySet);
				will(returnValue(tasks));

				oneOf(task).getId();
				will(returnValue(uuid));

				oneOf(scheduler).schedule(with(any(Runnable.class)), with(any(Long.class)), with(any(TimeUnit.class)));
				will(returnValue(future));

			}
		});
	}

	@After
	public void tearDown() throws Exception {
		Hazeltask.instances.clear();
	}

	@Test
	public void testGetInstanceByName() {
		assertNull(Hazeltask.getInstanceByName("test"));
		HazeltaskInstance instance = Hazeltask.newHazeltaskInstance("test", hazelcast, poolConfig, scheduler,
				serviceProvider);
		HazeltaskInstance instance2 = Hazeltask.getInstanceByName("test");
		assertEquals(instance, instance2);
	}

	@Test
	public void testNewHazeltaskInstance() {
		HazeltaskInstance instance = Hazeltask.newHazeltaskInstance("test", hazelcast, poolConfig, scheduler,
				serviceProvider);
		HazeltaskInstance instance2 = Hazeltask.newHazeltaskInstance("test", hazelcast, poolConfig, scheduler,
				serviceProvider);
		assertEquals(instance, instance2);
	}

}
