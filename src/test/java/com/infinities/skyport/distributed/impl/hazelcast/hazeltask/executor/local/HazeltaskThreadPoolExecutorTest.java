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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.ExecutorListener;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.AbortableTask;

public class HazeltaskThreadPoolExecutorTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private HazeltaskThreadPoolExecutor executor;
	private ExecutorListener listener1;
	private AbortableTask<String> task;
	private ThreadFactory factory;


	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		listener1 = context.mock(ExecutorListener.class, "listener1");
		task = context.mock(AbortableTask.class);
		factory = context.mock(ThreadFactory.class);
		executor = new HazeltaskThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), factory,
				new AbortPolicy());
	}

	@After
	public void tearDown() throws Exception {
		executor.shutdown();
	}

	@Test
	public void testAddListener() {
		assertEquals(0, executor.listeners.size());
		executor.addListener(listener1);
		assertEquals(1, executor.listeners.size());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExecuteRunnable() {
		Runnable c = context.mock(Runnable.class);
		executor.addListener(listener1);
		executor.execute(c);

	}

	@Test
	public void testExecuteRunnable2() {
		context.checking(new Expectations() {

			{
				oneOf(factory).newThread(with(any(Runnable.class)));
				will(returnValue(new Thread()));
			}
		});
		executor.addListener(listener1);
		executor.execute(task);

	}

	@Test
	public void testBeforeExecuteThreadRunnable() {
		context.checking(new Expectations() {

			{
				oneOf(listener1).beforeExecute(null, task);
			}
		});
		executor.addListener(listener1);
		executor.beforeExecute(null, task);
	}

	@Test
	public void testAfterExecuteRunnableThrowable() {
		final Exception t = new IllegalStateException();
		context.checking(new Expectations() {

			{
				oneOf(listener1).afterExecute(task, t);
			}
		});
		executor.addListener(listener1);
		executor.afterExecute(task, t);
	}

}
