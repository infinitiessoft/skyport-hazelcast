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
import java.util.Collection;

import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.TaskResponse;

public interface IExecutorTopologyService extends Serializable {

	boolean addPendingTask(HazeltaskTask<?> task, boolean replaceIfExists);

	IMap<String, HazeltaskTask<?>> getPendingTasks();

	Collection<String> getLocalPendingTaskKeys(String predicate);

	Collection<String> getLocalPendingTaskKeys();

	Collection<HazeltaskTask<?>> getLocalPendingTasks(String predicate);

	Collection<HazeltaskTask<?>> getLocalPendingTasks();

	/**
	 * Get the local partition's size of the pending work map TODO: should this
	 * live in a different Service class?
	 * 
	 * @return
	 */
	int getLocalPendingTaskMapSize();

	/**
	 * 
	 * @param task
	 * @return true if removed, false it did not exist
	 */
	boolean removePendingTask(HazeltaskTask<?> task);

	boolean removePendingTask(String taskId);

	void broadcastTaskCompletion(String taskId, Object response);

	<T> boolean broadcastTaskCompletion(String taskId, String member, T response);

	void broadcastTaskCancellation(String taskId);

	boolean broadcastTaskCancellation(String taskId, String member);

	void broadcastTaskError(String taskId, Throwable exception);

	boolean broadcastTaskError(String taskId, String member, Throwable exception);

	void addTaskResponseMessageHandler(MessageListener<TaskResponse<Object>> listener);

	// void clearQueue();

	boolean cancelTask(String taskId);

	void addLocalEntryListener(EntryListener<String, HazeltaskTask<?>> listener);

	Member getLocalMember();

}
