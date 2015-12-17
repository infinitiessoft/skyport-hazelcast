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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.TaskResponse;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.TaskResponse.Status;

public class DistributedFutureTracker implements MessageListener<TaskResponse<Object>>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(DistributedFutureTracker.class);
	protected Cache<String, DistributedFuture<Object>> futures;
	private final IExecutorTopologyService topologyService;


	public DistributedFutureTracker(final IExecutorTopologyService topologyService, long maxFutureWaitTime, TimeUnit unit) {
		this.topologyService = topologyService;
		futures = CacheBuilder.newBuilder()
				// no future will wait for more than this time
				.expireAfterAccess(maxFutureWaitTime, unit)
				.removalListener(new RemovalListener<String, DistributedFuture<Object>>() {

					@Override
					public void onRemoval(RemovalNotification<String, DistributedFuture<Object>> notification) {
						if (notification.getCause() == RemovalCause.EXPIRED) {
							DistributedFuture<Object> future = notification.getValue();
							long waitTimeMillis = System.currentTimeMillis() - future.getCreatedTime();
							notification.getValue().setException(
									new TimeoutException("Future timed out waiting.  Waited "
											+ (TimeUnit.MILLISECONDS.toMinutes(waitTimeMillis)) + " minutes"));

							topologyService.cancelTask(future.getTaskId());
							topologyService.removePendingTask(future.getTaskId());
						} else if (notification.getCause() == RemovalCause.COLLECTED) {
							// future was GC'd because we didn't
							// want to track it
							logger.debug("Future {} was garabge collected and removed from the tracker",
									notification.getKey());
						}
					}
				}).build();
	}

	@SuppressWarnings("unchecked")
	public <T> DistributedFuture<T> createFuture(HazeltaskTask<T> task) {
		DistributedFuture<T> future = new SimpleDistributedFuture<T>(topologyService, task.getId());
		this.futures.put(task.getId(), (DistributedFuture<Object>) future);
		return future;
	}

	public DistributedFuture<Object> remove(String id) {
		if (futures.getIfPresent(id) != null) {
			synchronized (futures) {
				DistributedFuture<Object> f = futures.getIfPresent(id);
				futures.invalidate(id);
				return f;
			}
		} else {
			return null;
		}
	}

	public Set<String> getTrackedTaskIds() {
		return new HashSet<String>(futures.asMap().keySet());
	}

	@Override
	public void onMessage(Message<TaskResponse<Object>> message) {
		logger.debug("receive task {} response", message.getMessageObject().getTaskId());
		TaskResponse<Object> response = message.getMessageObject();
		String taskId = response.getTaskId();
		DistributedFuture<Object> future = remove(taskId);
		if (future != null) {
			if (response.getStatus() == Status.FAILURE) {
				logger.debug("receive task {} failed message", message.getMessageObject().getTaskId());
				future.setException(response.getError());
			} else if (response.getStatus() == Status.SUCCESS) {
				logger.debug("receive task {} success message", new Object[] { message.getMessageObject().getTaskId() });
				future.set(response.getResponse());
			} else if (response.getStatus() == Status.CANCELLED) {
				logger.debug("receive task {} cancelled message", message.getMessageObject().getTaskId());
				// TODO: add a status for INTERRUPTED
				future.setCancelled(false);
			}
		}
	}

	public boolean cancelTask(String taskId) {
		DistributedFuture<Object> future = remove(taskId);
		if (future != null) {
			logger.debug("receive task {} cancel message", new Object[] { taskId });
			future.setCancelled(false);
			return true;
		}
		return false;
	}

	public boolean completeTask(String taskId, Object result) {
		DistributedFuture<Object> future = remove(taskId);
		if (future != null) {
			logger.debug("receive task {} success message: {}", new Object[] { taskId, result });
			future.set(result);
			return true;
		}
		return false;
	}

	public boolean failTask(String taskId, Throwable throwable) {
		DistributedFuture<Object> future = remove(taskId);
		if (future != null) {
			logger.debug("receive task {} failed message", taskId);
			future.setException(throwable);
			return true;
		}
		return false;
	}

	/**
	 * handles when a member leaves and hazelcast partition data is lost. We
	 * want to find the Futures that are waiting on lost data and error them
	 */
	public void errorFuture(String taskId, Exception e) {
		logger.warn(
				"task {} is encounter error, it might be happen when a member leaves and hazelcast partition data is lost",
				taskId);
		failTask(taskId, e);
	}

	public int size() {
		return (int) futures.size();
	}

	public synchronized void shutdown() {
		for (String uuid : futures.asMap().keySet()) {
			cancelTask(uuid);
		}
		futures.invalidateAll();
	}

}
