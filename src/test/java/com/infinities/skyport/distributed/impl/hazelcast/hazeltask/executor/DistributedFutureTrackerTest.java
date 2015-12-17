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
import static org.junit.Assert.assertNotEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTaskImpl;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.TaskResponse;

public class DistributedFutureTrackerTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private DistributedFutureTracker tracker;
	private IExecutorTopologyService service;
	private String uuid1;
	private Member local;
	private UUID memberid;


	@Before
	public void setUp() throws Exception {
		uuid1 = UUID.randomUUID().toString();
		memberid = UUID.randomUUID();
		service = context.mock(IExecutorTopologyService.class);
		tracker = new DistributedFutureTracker(service, 10, TimeUnit.SECONDS);
		local = context.mock(Member.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testFutureTrackSuccess() throws InterruptedException, ExecutionException {
		context.checking(new Expectations() {

			{
				oneOf(local).getUuid();
				will(returnValue(memberid.toString()));
			}
		});
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);

		TaskResponse<Object> response = new TaskResponse<Object>(local, uuid1, "good", TaskResponse.Status.SUCCESS);

		Message<TaskResponse<Object>> responseMessage = new Message<TaskResponse<Object>>("default-topic", response,
				System.currentTimeMillis(), local);
		tracker.onMessage(responseMessage);

		assertEquals(future.get(), "good");
	}

	@Test(expected = CancellationException.class)
	public void testFutureTrackCancelled() throws Throwable {
		context.checking(new Expectations() {

			{
				oneOf(local).getUuid();
				will(returnValue(memberid.toString()));
			}
		});
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		TaskResponse<Object> response = new TaskResponse<Object>(local, uuid1, "good", TaskResponse.Status.CANCELLED);

		Message<TaskResponse<Object>> responseMessage = new Message<TaskResponse<Object>>("default-topic", response,
				System.currentTimeMillis(), local);
		tracker.onMessage(responseMessage);

		try {
			future.get();
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFutureTrackException() throws Throwable {
		context.checking(new Expectations() {

			{
				oneOf(local).getUuid();
				will(returnValue(memberid.toString()));
			}
		});
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		TaskResponse<Object> response = new TaskResponse<Object>(local, uuid1, new IllegalArgumentException("bad"));

		Message<TaskResponse<Object>> responseMessage = new Message<TaskResponse<Object>>("default-topic", response,
				System.currentTimeMillis(), local);
		tracker.onMessage(responseMessage);

		try {
			future.get();
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test(expected = TimeoutException.class)
	public void testFutureTrackGetTimeout() throws InterruptedException, ExecutionException, TimeoutException {
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		Assert.assertEquals(future.get(10, TimeUnit.MILLISECONDS), "Yay!");
	}

	@Test
	public void testCreateFuture() {
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		assertEquals(uuid1, future.getTaskId());
	}

	@Test
	public void testRemove() {
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		assertEquals(1, tracker.size());
		DistributedFuture<Object> future2 = tracker.remove(task.getId());
		assertEquals(future, future2);
		assertEquals(0, tracker.size());
	}

	@Test
	public void testGetTrackedTaskIds() {
		Set<String> sets = new HashSet<String>();
		sets.add(uuid1);
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		tracker.createFuture(task);
		assertEquals(sets, tracker.getTrackedTaskIds());
		tracker.remove(task.getId());
		assertNotEquals(sets, tracker.getTrackedTaskIds());
	}

	@Test(expected = CancellationException.class)
	public void testCancelTask() throws Throwable {
		context.checking(new Expectations() {

			{
				oneOf(local).getUuid();
				will(returnValue(memberid.toString()));
			}
		});
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		tracker.cancelTask(uuid1);

		try {
			future.get();
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test
	public void testCompleteTask() throws Throwable {
		context.checking(new Expectations() {

			{
				oneOf(local).getUuid();
				will(returnValue(memberid.toString()));
			}
		});
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		tracker.completeTask(uuid1, "good");

		try {
			assertEquals("good", future.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailTask() throws Throwable {
		context.checking(new Expectations() {

			{
				oneOf(local).getUuid();
				will(returnValue(memberid.toString()));
			}
		});
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		tracker.failTask(uuid1, new IllegalArgumentException());

		try {
			assertEquals("good", future.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testErrorFuture() throws Throwable {
		context.checking(new Expectations() {

			{
				oneOf(local).getUuid();
				will(returnValue(memberid.toString()));
			}
		});
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		tracker.errorFuture(uuid1, new IllegalArgumentException());

		try {
			assertEquals("good", future.get());
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

	@Test
	public void testSize() {
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		assertEquals(1, tracker.size());
		DistributedFuture<Object> future2 = tracker.remove(task.getId());
		assertEquals(future, future2);
		assertEquals(0, tracker.size());
	}

	@Test(expected = CancellationException.class)
	public void testShutdown() throws Throwable {
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), (Callable<String>) null);
		DistributedFuture<String> future = tracker.createFuture(task);
		assertEquals(1, tracker.size());
		tracker.shutdown();
		assertEquals(0, tracker.size());
		try {
			future.get();
		} catch (ExecutionException e) {
			throw e.getCause();
		}
	}

}
