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
package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.infinities.skyport.ServiceProvider;
import com.infinities.skyport.async.AsyncTask;
import com.infinities.skyport.distributed.DistributedAtomicLong;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.HazeltaskServiceListener;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.NamedThreadFactory;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.local.LocalTaskExecutorService;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.AbortableTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.model.PoolConfig;
import com.infinities.skyport.model.PoolSize;

public class DistributedExecutorServiceImplTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private DistributedExecutorServiceImpl service;
	private IExecutorTopologyService topologyService;
	private DistributedFutureTracker futureTracker;
	private LocalTaskExecutorService localExecutorService;
	private ServiceProvider serviceProvider;
	private HazeltaskServiceListener<DistributedExecutorService> listener;
	private DistributedAtomicLong l;
	private AbortableTask<?> task;
	private String uuid1, memberid;
	private Member local;

	private Set<String> localKeys;
	private IMap<String, HazeltaskTask<?>> pendingTasks;


	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		pendingTasks = context.mock(IMap.class);
		local = context.mock(Member.class);
		uuid1 = UUID.randomUUID().toString();
		localKeys = new HashSet<String>();
		localKeys.add(uuid1);
		memberid = UUID.randomUUID().toString();
		l = context.mock(DistributedAtomicLong.class);
		task = context.mock(AbortableTask.class);
		listener = new HazeltaskServiceListener<DistributedExecutorService>() {

			@Override
			public void onBeginStart(DistributedExecutorService svc) {
				l.set(1);
			}

			@Override
			public void onEndStart(DistributedExecutorService svc) {
				l.set(2);
			}

			@Override
			public void onBeginShutdown(DistributedExecutorService svc) {
				l.set(3);
			}

			@Override
			public void onEndShutdown(DistributedExecutorService svc) {
				l.set(4);
			}
		};
		topologyService = context.mock(IExecutorTopologyService.class);
		serviceProvider = context.mock(ServiceProvider.class);
		futureTracker = new DistributedFutureTracker(topologyService, 10, TimeUnit.SECONDS);

		localExecutorService = new LocalTaskExecutorService(new NamedThreadFactory("test", "group"), topologyService,
				new PoolConfig(PoolSize.SHORT), serviceProvider);
		service = new DistributedExecutorServiceImpl("test", topologyService, futureTracker, localExecutorService);
		service.addServiceListener(listener);
	}

	@After
	public void tearDown() throws Exception {
		service.destroy();
	}

	@Test
	public void testShutdown() {
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

			}
		});
		service.shutdown();
	}

	@Test
	public void testShutdownNow() {
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

				allowing(task).getId();
				will(returnValue("id"));

				allowing(topologyService).getLocalPendingTaskKeys();
				will(returnValue(new ArrayList<String>()));
			}
		});
		localExecutorService.getQueue().add(task);
		List<Runnable> list = service.shutdownNow();
		assertEquals(1, list.size());
	}

	@Test
	public void testDoShutdownNow() {
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

				allowing(task).getId();
				will(returnValue("id"));

				allowing(topologyService).getLocalPendingTaskKeys();
				will(returnValue(new ArrayList<String>()));

			}
		});
		localExecutorService.getQueue().add(task);
		List<HazeltaskTask<?>> list = service.doShutdownNow(true);
		assertEquals(1, list.size());
	}

	@Test
	public void testDoShutdownNow2() {
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

				allowing(task).getId();
				will(returnValue("id"));

				allowing(topologyService).getLocalPendingTaskKeys();
				will(returnValue(new ArrayList<String>()));

				allowing(task).interrupt();
				allowing(task).abort();
				allowing(task).run();

				allowing(task).isInterrupted();
				will(returnValue(true));

				allowing(task).isAbort();
				will(returnValue(true));

			}
		});
		localExecutorService.getQueue().add(task);
		List<HazeltaskTask<?>> list = service.doShutdownNow(false);
		assertEquals(0, list.size());
	}

	@Test
	public void testIsShutdown() {

		assertFalse(service.isShutdown());
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

				allowing(task).getId();
				will(returnValue(uuid1));

				oneOf(topologyService).getLocalPendingTaskKeys();
				will(returnValue(localKeys));

				exactly(3).of(topologyService).getPendingTasks();
				will(returnValue(pendingTasks));

				oneOf(pendingTasks).tryLock(uuid1);
				will(returnValue(true));

				oneOf(pendingTasks).containsKey(uuid1);
				will(returnValue(true));

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

				exactly(1).of(topologyService).broadcastTaskCompletion(uuid1, memberid, "good");
				will(returnValue(true));

				oneOf(topologyService).removePendingTask(uuid1);
				will(returnValue(true));

				oneOf(pendingTasks).unlock(uuid1);

				allowing(task).getId();
				will(returnValue("id"));
			}
		});
		localExecutorService.getQueue().add(task);
		List<HazeltaskTask<?>> list = service.doShutdownNow(false);
		assertEquals(0, list.size());
		assertTrue(service.isShutdown());
	}

	@Test
	public void testIsTerminated() {
		assertFalse(service.isTerminated());
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

				allowing(task).getId();
				will(returnValue("id"));

				allowing(topologyService).getLocalPendingTaskKeys();
				will(returnValue(new ArrayList<String>()));

				allowing(task).interrupt();
				allowing(task).abort();
				allowing(task).run();

				allowing(task).isInterrupted();
				will(returnValue(true));

				allowing(task).isAbort();
				will(returnValue(true));

			}
		});
		localExecutorService.getQueue().add(task);
		List<HazeltaskTask<?>> list = service.doShutdownNow(false);
		assertEquals(0, list.size());
		assertTrue(service.isTerminated());

	}

	@Test(expected = IllegalStateException.class)
	public void testAwaitTerminationWithoutShutdown() throws InterruptedException {
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

			}
		});
		assertTrue(service.awaitTermination(5, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testAwaitTerminationWithoutShutdown2() throws InterruptedException {
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

			}
		});
		service.shutdown();
		assertTrue(service.awaitTermination(5, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testAwaitTerminationWithoutShutdown3() throws InterruptedException {
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

			}
		});
		@SuppressWarnings("unchecked")
		DistributedFuture<Object> future = context.mock(DistributedFuture.class);
		service.getDistributedFutureTracker().futures.put(UUID.randomUUID().toString(), future);
		service.shutdown();
		assertFalse(service.awaitTermination(5, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testSubmitCallableOfT() throws InterruptedException, ExecutionException {
		assertEquals(0, futureTracker.futures.size());
		context.checking(new Expectations() {

			{
				oneOf(topologyService).getLocalMember();
				will(returnValue(local));

				oneOf(local).getUuid();
				will(returnValue(memberid));

				oneOf(topologyService).addPendingTask(with(any(HazeltaskTask.class)), with(any(Boolean.class)));
				will(returnValue(true));

				oneOf(l).set(3L);
				oneOf(l).set(4L);
			}
		});
		MyCallable c = new MyCallable();
		service.submit(c);
		assertEquals(1, futureTracker.futures.size());

	}

	@Test
	public void testSubmitDistributeTask() {
		@SuppressWarnings("unchecked")
		final AsyncTask<String> task = context.mock(AsyncTask.class);
		assertEquals(0, futureTracker.futures.size());
		context.checking(new Expectations() {

			{
				oneOf(topologyService).getLocalMember();
				will(returnValue(local));

				oneOf(local).getUuid();
				will(returnValue(memberid.toString()));

				oneOf(topologyService).addPendingTask(with(any(HazeltaskTask.class)), with(any(Boolean.class)));
				will(returnValue(true));

				oneOf(l).set(3L);
				oneOf(l).set(4L);

			}
		});

		service.submit(task);
		assertEquals(1, futureTracker.futures.size());
	}

	@Test
	public void testStartup() {
		context.checking(new Expectations() {

			{
				oneOf(l).set(1L);
				oneOf(l).set(2L);
				oneOf(l).set(3L);
				oneOf(l).set(4L);
			}
		});
		service.startup();
	}

	@Test
	public void testAddServiceListener() {
		assertEquals(1, service.listeners.size());
		final DistributedAtomicLong l2 = context.mock(DistributedAtomicLong.class, "l2");
		HazeltaskServiceListener<DistributedExecutorService> listener2;
		listener2 = new HazeltaskServiceListener<DistributedExecutorService>() {

			@Override
			public void onBeginStart(DistributedExecutorService svc) {
				l2.set(1);
			}

			@Override
			public void onEndStart(DistributedExecutorService svc) {
				l2.set(2);
			}

			@Override
			public void onBeginShutdown(DistributedExecutorService svc) {
				l2.set(3);
			}

			@Override
			public void onEndShutdown(DistributedExecutorService svc) {
				l2.set(4);
			}
		};
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

				oneOf(l2).set(3L);
				oneOf(l2).set(4L);

			}
		});
		service.addServiceListener(listener2);
		assertEquals(2, service.listeners.size());
	}

	@Test
	public void testGetLocalTaskExecutorService() {
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

			}
		});
		assertEquals(localExecutorService, service.getLocalTaskExecutorService());
	}

	@Test
	public void testGetDistributedFutureTracker() {
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

			}
		});
		assertEquals(futureTracker, service.getDistributedFutureTracker());
	}

	@Test
	public void testDestroy() {
		context.checking(new Expectations() {

			{
				oneOf(l).set(3L);
				oneOf(l).set(4L);

			}
		});
		service.destroy();
	}


	private class MyCallable implements Callable<String>, Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;


		@Override
		public String call() throws Exception {
			return "good";
		}

	}

}
