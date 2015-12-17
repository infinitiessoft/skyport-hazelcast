package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.IMap;
import com.infinities.skyport.ServiceProvider;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.NamedThreadFactory;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.IExecutorTopologyService;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.AbortableTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTaskImpl;
import com.infinities.skyport.model.PoolConfig;
import com.infinities.skyport.model.PoolSize;

public class LocalTaskExecutorServiceTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private LocalTaskExecutorService service;
	private NamedThreadFactory factory;
	private IExecutorTopologyService topologyService;
	private ServiceProvider serviceProvider;
	private String uuid1, uuid2, memberid;


	@Before
	public void setUp() throws Exception {
		uuid1 = UUID.randomUUID().toString();
		uuid2 = UUID.randomUUID().toString();
		memberid = UUID.randomUUID().toString();
		factory = new NamedThreadFactory("test", "group");
		topologyService = context.mock(IExecutorTopologyService.class);
		serviceProvider = context.mock(ServiceProvider.class);
		service = new LocalTaskExecutorService(factory, topologyService, new PoolConfig(PoolSize.SHORT), serviceProvider);
	}

	@After
	public void tearDown() throws Exception {
		service.shutdown();
	}

	@Test
	public void testGetQueueSize() {
		assertEquals(0, service.getQueueSize());
	}

	@Test
	public void testExecute() {
		final Set<String> keys = new HashSet<String>();
		final HazeltaskTask<String> task = newTask();
		@SuppressWarnings("unchecked")
		final IMap<String, HazeltaskTask<String>> map = context.mock(IMap.class);
		final Map<String, HazeltaskTask<String>> map2 = new HashMap<String, HazeltaskTask<String>>();
		keys.add(uuid1);
		map2.put(uuid1, task);
		context.checking(new Expectations() {

			{

				exactly(1).of(topologyService).getLocalPendingTaskKeys();
				will(returnValue(keys));

				exactly(4).of(topologyService).getPendingTasks();
				will(returnValue(map));

				exactly(1).of(map).tryLock(uuid1);
				will(returnValue(true));

				exactly(1).of(map).containsKey(uuid1);
				will(returnValue(true));

				exactly(1).of(map).set(uuid1, task);

				exactly(1).of(topologyService).broadcastTaskCompletion(uuid1, memberid.toString(), "good");
				will(returnValue(true));

				exactly(1).of(topologyService).removePendingTask(uuid1);
				will(returnValue(true));

				exactly(1).of(map).unlock(uuid1);
			}
		});
		service.execute(task);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteDistributedTask() {
		final HazeltaskTask<String> task = context.mock(HazeltaskTask.class);
		final Set<String> keys = new HashSet<String>();
		final IMap<String, HazeltaskTask<String>> map = context.mock(IMap.class);
		final Map<String, HazeltaskTask<String>> map2 = new HashMap<String, HazeltaskTask<String>>();
		keys.add(uuid1);
		map2.put(uuid1, task);
		context.checking(new Expectations() {

			{
				oneOf(task).setServiceProvider(serviceProvider);

				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(task).isStarted();
				will(returnValue(false));

				allowing(task).setStarted(true);

				allowing(task).run();

				exactly(1).of(topologyService).getLocalPendingTaskKeys();
				will(returnValue(keys));

				exactly(4).of(topologyService).getPendingTasks();
				will(returnValue(map));

				exactly(1).of(map).tryLock(uuid1);
				will(returnValue(true));

				exactly(1).of(map).containsKey(uuid1);
				will(returnValue(true));

				exactly(1).of(map).set(uuid1, task);

				exactly(1).of(topologyService).broadcastTaskCompletion(uuid1, memberid.toString(), "good");
				will(returnValue(true));

				exactly(1).of(topologyService).removePendingTask(uuid1);
				will(returnValue(true));

				exactly(1).of(map).unlock(uuid1);

			}
		});

		service.execute(task);
	}

	@Test
	public void testShutdown() {
		service.shutdown();
	}

	@Test
	public void testShutdownNow() {
		service.shutdownNow();
	}

	@Test
	public void testIsShutdown() {
		assertFalse(service.isShutdown());
		service.shutdown();
		assertTrue(service.isShutdown());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCancelTask() {
		final AbortableTask<String> task = context.mock(AbortableTask.class);
		context.checking(new Expectations() {

			{

				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(task).getFromMember();
				will(returnValue(memberid));

				allowing(task).run();

				oneOf(topologyService).broadcastTaskCancellation(uuid1, memberid.toString());
				will(returnValue(true));
			}
		});

		assertFalse(service.contains(task));
		service.getQueue().add(task);
		assertTrue(service.contains(task));
		assertTrue(service.cancelTask(uuid1));
	}

	@Test
	public void testGetMaximumPoolSize() {
		assertEquals(15, service.getMaximumPoolSize());
	}

	@Test
	public void testGetCorePoolSize() {
		assertEquals(15, service.getCorePoolSize());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetQueue() {
		final AbortableTask<String> task = context.mock(AbortableTask.class);
		final Set<String> keys = new HashSet<String>();
		final IMap<String, HazeltaskTask<String>> map = context.mock(IMap.class);
		final Map<String, HazeltaskTask<String>> map2 = new HashMap<String, HazeltaskTask<String>>();
		keys.add(uuid1);
		map2.put(uuid1, task);
		context.checking(new Expectations() {

			{

				allowing(topologyService).getLocalPendingTaskKeys();
				will(returnValue(keys));

				allowing(topologyService).getPendingTasks();
				will(returnValue(map));

				allowing(map).tryLock(uuid1);
				will(returnValue(true));

				allowing(map).containsKey(uuid1);
				will(returnValue(true));

				allowing(map).set(uuid1, task);

				oneOf(topologyService).broadcastTaskCompletion(uuid1, memberid, "good");
				will(returnValue(true));

				oneOf(topologyService).removePendingTask(uuid1);
				will(returnValue(true));

				allowing(map).unlock(uuid1);

				allowing(task).getId();
				will(returnValue(uuid1));

				oneOf(task).run();

				exactly(3).of(task).isInterrupted();
				will(returnValue(false));

				exactly(1).of(task).isAbort();
				will(returnValue(false));

				exactly(2).of(task).getFromMember();
				will(returnValue(memberid));

				exactly(1).of(task).getException();
				will(returnValue(null));

				exactly(1).of(task).getResult();
				will(returnValue("good"));
			}
		});
		assertFalse(service.contains(task));
		service.getQueue().add(task);
		assertTrue(service.contains(task));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetTasksInProcess() {
		final AbortableTask<String> task2 = context.mock(AbortableTask.class);
		context.checking(new Expectations() {

			{

				allowing(task2).getId();
				will(returnValue(uuid1));

			}
		});
		assertFalse(service.contains(task2));
		service.tasksInProgressTracker.tasksInProgress.put(task2.getId(), task2);
		assertTrue(service.contains(task2));
		assertTrue(service.getTasksInProcess().contains(task2));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testContains() {
		final AbortableTask<String> task = context.mock(AbortableTask.class, "task");
		final AbortableTask<String> task2 = context.mock(AbortableTask.class);
		context.checking(new Expectations() {

			{

				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(task2).getId();
				will(returnValue(uuid2));

			}
		});
		assertFalse(service.contains(task));
		service.getQueue().add(task);
		assertTrue(service.contains(task));
		assertFalse(service.contains(task2));
		service.tasksInProgressTracker.tasksInProgress.put(task2.getId(), task2);
		assertTrue(service.contains(task2));
	}

	private HazeltaskTask<String> newTask() {
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), new Callable<String>() {

			@Override
			public String call() throws Exception {
				return "good";
			}
		});
		return task;
	}

	// private HazeltaskTask newTask2() {
	// HazeltaskTask task = new HazeltaskTaskImpl<String, String>(uuid2,
	// memberid.toString(), new Callable<String>() {
	//
	// @Override
	// public String call() throws Exception {
	// return "good";
	// }
	// });
	// return task;
	// }

}
