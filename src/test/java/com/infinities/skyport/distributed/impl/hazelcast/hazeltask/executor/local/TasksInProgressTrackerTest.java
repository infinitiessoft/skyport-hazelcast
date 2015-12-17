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
package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.local;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.IMap;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.IExecutorTopologyService;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.AbortableTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.Task;

//@Category(IntegrationTest.class)
public class TasksInProgressTrackerTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private TasksInProgressTracker tracker;
	private IExecutorTopologyService service;
	private String uuid1;
	private String memberid;
	private Thread t;
	private AbortableTask<String> task;
	private Set<String> set;
	private IMap<String, Task<String>> imap;


	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		uuid1 = UUID.randomUUID().toString();
		memberid = UUID.randomUUID().toString();
		service = context.mock(IExecutorTopologyService.class);
		tracker = new TasksInProgressTracker(service);
		t = context.mock(Thread.class);
		task = context.mock(AbortableTask.class);
		set = new HashSet<String>();
		set.add(uuid1);
		imap = context.mock(IMap.class);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testBeforeExecute() {
		context.checking(new Expectations() {

			{
				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(service).getLocalPendingTaskKeys();
				will(returnValue(set));

				allowing(service).getPendingTasks();
				will(returnValue(imap));

				allowing(imap).tryLock(uuid1);
				will(returnValue(true));

				allowing(imap).containsKey(uuid1);
				will(returnValue(true));
			}
		});
		tracker.beforeExecute(t, task);
	}

	@Test
	public void testBeforeExecute2() {
		context.checking(new Expectations() {

			{
				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(service).getLocalPendingTaskKeys();
				will(returnValue(set));

				allowing(service).getPendingTasks();
				will(returnValue(imap));

				allowing(imap).tryLock(uuid1);
				will(returnValue(false));

				allowing(imap).containsKey(uuid1);
				will(returnValue(true));

				oneOf(task).interrupt();
			}
		});
		tracker.beforeExecute(t, task);
	}

	@Test
	public void testBeforeExecute3() {
		context.checking(new Expectations() {

			{
				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(service).getLocalPendingTaskKeys();
				will(returnValue(set));

				allowing(service).getPendingTasks();
				will(returnValue(imap));

				allowing(imap).tryLock(uuid1);
				will(returnValue(true));

				allowing(imap).containsKey(uuid1);
				will(returnValue(false));

				oneOf(task).abort();
			}
		});
		tracker.beforeExecute(t, task);
	}

	@Test
	public void testBeforeExecute4() {
		tracker.tasksInProgress.put(uuid1, task);

		context.checking(new Expectations() {

			{
				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(service).getLocalPendingTaskKeys();
				will(returnValue(set));

				allowing(service).getPendingTasks();
				will(returnValue(imap));

				allowing(imap).tryLock(uuid1);
				will(returnValue(true));

				allowing(imap).containsKey(uuid1);
				will(returnValue(true));

				oneOf(task).interrupt();

				oneOf(task).abort();
			}
		});
		tracker.beforeExecute(t, task);
	}

	@Test
	public void testBeforeExecute5() {
		set = new HashSet<String>();
		context.checking(new Expectations() {

			{
				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(service).getLocalPendingTaskKeys();
				will(returnValue(set));

				allowing(service).getPendingTasks();
				will(returnValue(imap));

				allowing(imap).tryLock(uuid1);
				will(returnValue(true));

				allowing(imap).containsKey(uuid1);
				will(returnValue(false));

				oneOf(task).interrupt();
			}
		});
		tracker.beforeExecute(t, task);
	}

	@Test
	public void testAfterExecute2() {
		context.checking(new Expectations() {

			{
				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(task).isAbort();
				will(returnValue(true));

				allowing(service).getPendingTasks();
				will(returnValue(imap));

				allowing(imap).containsKey(uuid1);
				will(returnValue(true));

				allowing(service).removePendingTask(uuid1);
				will(returnValue(true));

				allowing(imap).unlock(uuid1);

				oneOf(task).isInterrupted();
				will(returnValue(true));
			}
		});
		tracker.afterExecute(task, null);
	}

	@Test
	public void testAfterExecute3() {
		context.checking(new Expectations() {

			{
				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(task).isAbort();
				will(returnValue(false));

				allowing(task).getException();
				will(returnValue(null));

				allowing(task).getFromMember();
				will(returnValue(memberid.toString()));

				allowing(task).getResult();
				will(returnValue("good"));

				allowing(service).getPendingTasks();
				will(returnValue(imap));

				allowing(imap).containsKey(uuid1);
				will(returnValue(true));

				allowing(service).removePendingTask(uuid1);
				will(returnValue(true));

				allowing(imap).unlock(uuid1);

				allowing(service).broadcastTaskCompletion(uuid1, memberid.toString(), "good");
				will(returnValue(true));

				allowing(task).isInterrupted();
				will(returnValue(true));
			}
		});
		tracker.afterExecute(task, null);
	}

	@Test
	public void testAfterExecute4() {
		final Exception t = new IllegalStateException();
		context.checking(new Expectations() {

			{
				allowing(task).getId();
				will(returnValue(uuid1));

				allowing(task).isAbort();
				will(returnValue(false));

				allowing(task).getException();
				will(returnValue(t));

				allowing(task).getFromMember();
				will(returnValue(memberid.toString()));

				allowing(task).getResult();
				will(returnValue("good"));

				allowing(service).getPendingTasks();
				will(returnValue(imap));

				allowing(imap).containsKey(uuid1);
				will(returnValue(true));

				allowing(service).removePendingTask(uuid1);
				will(returnValue(true));

				allowing(imap).unlock(uuid1);

				allowing(service).broadcastTaskError(uuid1, memberid.toString(), t);
				will(returnValue(true));

				allowing(task).isInterrupted();
				will(returnValue(true));
			}
		});
		tracker.afterExecute(task, null);
	}

	@Test
	public void testGetTasksInProgress() {
		assertEquals(0, tracker.getTasksInProgress().size());
		tracker.tasksInProgress.put(uuid1, task);
		assertEquals(1, tracker.getTasksInProgress().size());
	}

}
