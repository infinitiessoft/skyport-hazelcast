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

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.ExecutorListener;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.AbortableTask;

public class HazeltaskThreadPoolExecutor extends ThreadPoolExecutor {

	private static final Logger logger = LoggerFactory.getLogger(HazeltaskThreadPoolExecutor.class);
	protected final Collection<ExecutorListener> listeners = new CopyOnWriteArrayList<ExecutorListener>();


	public HazeltaskThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
	}

	public void addListener(ExecutorListener listener) {
		listeners.add(listener);
	}

	@Override
	public void execute(Runnable runnable) {
		if (!(runnable instanceof AbortableTask)) {
			throw new IllegalArgumentException("arg should be a AbortableTask");
		}
		super.execute(runnable);
	}

	@Override
	protected void beforeExecute(Thread t, Runnable runnable) {
		for (ExecutorListener listener : listeners) {
			try {
				listener.beforeExecute(t, (AbortableTask<?>) runnable);
			} catch (Throwable e) {
				// ignore and log
				logger.warn("An unexpected error occurred in the before Executor Listener", e);
			}
		}
	}

	@Override
	protected void afterExecute(Runnable runnable, Throwable exception) {
		for (ExecutorListener listener : listeners) {
			try {
				listener.afterExecute(((AbortableTask<?>) runnable), exception);
			} catch (Throwable e) {
				// ignore and log
				logger.warn("An unexpected error occurred in the after Executor Listener", e);
			}
		}
	}

}
