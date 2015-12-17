package com.infinities.skyport.distributed.impl.hazelcast;

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
import com.infinities.skyport.model.PoolConfig;
import com.infinities.skyport.model.PoolSize;

public class HazelcastThreadPoolTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private HazelcastThreadPool threadPool;
	private final String name = "name";
	// private HazelcastInstance instance;
	private ScheduledExecutorService executorService;
	private ServiceProvider serviceProvider;

	private HazelcastInstance hazelcast;
	private PartitionService partitionService;
	private Cluster cluster;
	private Member member;
	private IExecutorService executor;
	@SuppressWarnings("rawtypes")
	private IMap map;
	@SuppressWarnings("rawtypes")
	private ITopic topic;
	private LifecycleService lifecycleService;
	// private Map<UUID, HazeltaskTask> tasks;
	// private Set<UUID> localKeySet;
	private ScheduledFuture<?> future;


	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		hazelcast = context.mock(HazelcastInstance.class);
		// instance = context.mock(HazelcastInstance.class);
		executorService = context.mock(ScheduledExecutorService.class);
		serviceProvider = context.mock(ServiceProvider.class);
		partitionService = context.mock(PartitionService.class);
		cluster = context.mock(Cluster.class);
		member = context.mock(Member.class);
		executor = context.mock(IExecutorService.class);
		map = context.mock(IMap.class);
		topic = context.mock(ITopic.class);
		lifecycleService = context.mock(LifecycleService.class);
		// uuid = UUID.randomUUID();
		// localKeySet = new HashSet<UUID>();
		// localKeySet.add(uuid);
		// task = context.mock(SimpleTask.class);
		// tasks = new HashMap<UUID, HazeltaskTask>();
		future = context.mock(ScheduledFuture.class);

		context.checking(new Expectations() {

			{
				exactly(3).of(hazelcast).getPartitionService();
				will(returnValue(partitionService));

				exactly(3).of(hazelcast).getCluster();
				will(returnValue(cluster));

				exactly(3).of(cluster).getLocalMember();
				will(returnValue(member));

				oneOf(hazelcast).getExecutorService(name + "_" + PoolSize.LONG);
				will(returnValue(executor));

				oneOf(hazelcast).getExecutorService(name + "_" + PoolSize.LONG + "-com");
				will(returnValue(executor));

				oneOf(hazelcast).getExecutorService(name + "_" + PoolSize.MEDIUM);
				will(returnValue(executor));

				oneOf(hazelcast).getExecutorService(name + "_" + PoolSize.MEDIUM + "-com");
				will(returnValue(executor));

				oneOf(hazelcast).getExecutorService(name + "_" + PoolSize.SHORT);
				will(returnValue(executor));

				oneOf(hazelcast).getExecutorService(name + "_" + PoolSize.SHORT + "-com");
				will(returnValue(executor));

				oneOf(hazelcast).getMap(name + "_" + PoolSize.LONG + "-pending-tasks");
				will(returnValue(map));
				oneOf(hazelcast).getMap(name + "_" + PoolSize.MEDIUM + "-pending-tasks");
				will(returnValue(map));
				oneOf(hazelcast).getMap(name + "_" + PoolSize.SHORT + "-pending-tasks");
				will(returnValue(map));

				oneOf(hazelcast).getTopic(name + "_" + PoolSize.LONG + "-task-response");
				will(returnValue(topic));
				oneOf(hazelcast).getTopic(name + "_" + PoolSize.MEDIUM + "-task-response");
				will(returnValue(topic));
				oneOf(hazelcast).getTopic(name + "_" + PoolSize.SHORT + "-task-response");
				will(returnValue(topic));

				exactly(3).of(partitionService).addMigrationListener(with(any(MigrationListener.class)));
				will(returnValue("1"));

				exactly(3).of(topic).addMessageListener(with(any(DistributedFutureTracker.class)));
				will(returnValue("2"));

				exactly(3).of(map).addLocalEntryListener(with(any(EntryListener.class)));
				will(returnValue("good"));

				exactly(3).of(hazelcast).getLifecycleService();
				will(returnValue(lifecycleService));

				exactly(3).of(lifecycleService).addLifecycleListener(with(any(LifecycleListener.class)));
				will(returnValue("good"));

				exactly(3).of(lifecycleService).isRunning();
				will(returnValue(true));

				exactly(3).of(executorService).schedule(with(any(Runnable.class)), with(any(Long.class)),
						with(any(TimeUnit.class)));
				will(returnValue(future));
			}
		});

		threadPool = new HazelcastThreadPool(name, hazelcast, new PoolConfig(PoolSize.LONG),
				new PoolConfig(PoolSize.MEDIUM), new PoolConfig(PoolSize.SHORT), executorService, serviceProvider);

	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testShutdown() {
		threadPool.shutdown();
	}

	@Test
	public void testDestroy() {
		threadPool.destroy();
	}

}
