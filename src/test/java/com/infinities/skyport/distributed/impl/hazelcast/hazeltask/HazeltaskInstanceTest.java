package com.infinities.skyport.distributed.impl.hazelcast.hazeltask;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.DistributedFutureTracker;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.model.PoolConfig;
import com.infinities.skyport.model.PoolSize;

public class HazeltaskInstanceTest {

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
	// private UUID uuid;
	// private SimpleTask task;
	private HazeltaskInstance instance;


	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
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
		// uuid = UUID.randomUUID();
		localKeySet = new HashSet<UUID>();
		// localKeySet.add(uuid);
		// task = context.mock(SimpleTask.class);
		tasks = new HashMap<UUID, HazeltaskTask<?>>();
		// tasks.put(uuid, task);
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
			}
		});
		instance = new HazeltaskInstance("test", hazelcast, poolConfig, scheduler, serviceProvider);
	}

	@After
	public void tearDown() throws Exception {
		instance.shutdown();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testStart() {
		final ScheduledFuture<?> future = context.mock(ScheduledFuture.class);

		context.checking(new Expectations() {

			{
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

				oneOf(scheduler).schedule(with(any(Runnable.class)), with(any(Long.class)), with(any(TimeUnit.class)));
				will(returnValue(future));

			}
		});
		instance.start();
	}

	@Test
	public void testShutdown() {
		Hazeltask.instances.put("test", instance);
		instance.shutdown();
		assertTrue(instance.getExecutorService().isShutdown());
		assertEquals(0, instance.getFutureTracker().size());
		assertFalse(Hazeltask.instances.containsKey("test"));
	}

	@Test
	public void testGetTopologyName() {
		assertEquals("test", instance.getTopologyName());
	}

	@Test
	public void testGetExecutorService() {
		assertNotNull(instance.getExecutorService());
	}

	@Test
	public void testGetLocalExecutorService() {
		assertNotNull(instance.getLocalExecutorService());
	}

	@Test
	public void testGetFutureTracker() {
		assertNotNull(instance.getFutureTracker());
	}

}
