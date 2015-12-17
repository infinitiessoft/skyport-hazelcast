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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.hazelcast.query.SqlPredicate;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop.CancelReturnOp;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop.CancelTaskOp;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop.CompletedTaskOp;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop.FailedTaskOp;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.TaskResponse;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.hazelcast.MemberTasks;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.hazelcast.MemberTasks.MemberResponse;

public class HazelcastExecutorTopologyService implements IExecutorTopologyService {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(HazelcastExecutorTopologyService.class);
	private final String topologyName;
	private final Member me;
	private final IExecutorService communicationExecutorService;
	private final IMap<String, HazeltaskTask<?>> pendingTask;
	private final ITopic<TaskResponse<Object>> taskResponseTopic;
	private Cluster cluster;


	public HazelcastExecutorTopologyService(String topologyName, HazelcastInstance hazelcast) {
		this.topologyName = topologyName;
		this.cluster = hazelcast.getCluster();
		this.me = cluster.getLocalMember();
		this.communicationExecutorService = hazelcast.getExecutorService(name("com"));
		String pendingTaskMapName = name("pending-tasks");

		this.pendingTask = hazelcast.getMap(pendingTaskMapName);
		this.taskResponseTopic = hazelcast.getTopic(name("task-response"));
	}

	private String name(String name) {
		return topologyName + "-" + name;
	}

	@Override
	public boolean addPendingTask(HazeltaskTask<?> task, boolean replaceIfExists) {
		if (!replaceIfExists) {
			return pendingTask.putIfAbsent(task.getId(), task) == null;
		}
		pendingTask.put(task.getId(), task);
		return true;
	}

	@Override
	public boolean removePendingTask(HazeltaskTask<?> task) {
		return removePendingTask(task.getId());
	}

	@Override
	public boolean removePendingTask(String taskId) {
		boolean removed = pendingTask.remove(taskId) != null;
		logger.debug("remove pending task {} , task is present? {}", new Object[] { taskId, removed });
		// TODO try removeAsync
		// pendingTask.removeAsync(taskId);
		return removed;
	}

	@Override
	public void broadcastTaskCompletion(String taskId, Object response) {
		logger.debug("task {} is completed, broadcast completion message now", taskId);
		TaskResponse<Object> message = new TaskResponse<Object>(me, taskId, response, TaskResponse.Status.SUCCESS);
		taskResponseTopic.publish(message);

	}

	@Override
	public void broadcastTaskCancellation(String taskId) {
		logger.debug("task {} is cancelled, broadcast cancelled message now", taskId);
		TaskResponse<Object> message = new TaskResponse<Object>(me, taskId, null, TaskResponse.Status.CANCELLED);
		taskResponseTopic.publish(message);
	}

	@Override
	public void broadcastTaskError(String taskId, Throwable exception) {
		logger.debug("task {} encounter error, broadcast Error message now", taskId);
		TaskResponse<Object> message = new TaskResponse<Object>(me, taskId, exception);
		taskResponseTopic.publish(message);
	}

	@Override
	public Set<String> getLocalPendingTaskKeys(String predicate) {
		Set<String> keys = pendingTask.localKeySet(new SqlPredicate(predicate));
		return keys;
	}

	@Override
	public Set<String> getLocalPendingTaskKeys() {
		Set<String> keys = pendingTask.localKeySet();
		return keys;
	}

	@Override
	public int getLocalPendingTaskMapSize() {
		return pendingTask.localKeySet().size();
	}

	@Override
	public void addTaskResponseMessageHandler(MessageListener<TaskResponse<Object>> listener) {
		taskResponseTopic.addMessageListener(listener);
	}

	// @Override
	// public void clearQueue() {
	// MemberTasks.executeOptimistic(communicationExecutorService,
	// cluster.getMembers(), new ClearGroupQueueOp(topology
	// .getName()));
	// }

	@Override
	public boolean cancelTask(String taskId) {
		Collection<MemberResponse<Boolean>> responses = MemberTasks.executeOptimistic(communicationExecutorService,
				cluster.getMembers(), new CancelTaskOp(topologyName, taskId));

		for (MemberResponse<Boolean> response : responses) {
			if (response.getValue()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public IMap<String, HazeltaskTask<?>> getPendingTasks() {
		return pendingTask;
	}

	@Override
	public void addLocalEntryListener(EntryListener<String, HazeltaskTask<?>> listener) {
		pendingTask.addLocalEntryListener(listener);
	}

	@Override
	public Member getLocalMember() {
		return me;
	}

	@Override
	public <T> boolean broadcastTaskCompletion(String taskId, String member, T result) {
		Set<Member> members = cluster.getMembers();
		for (Member m : members) {
			if (member.equals(m.getUuid())) {
				MemberResponse<Boolean> response = MemberTasks.executeOptimistic(communicationExecutorService, m,
						new CompletedTaskOp<T>(topologyName, taskId, result));

				if (response != null && response.getValue()) {
					return true;
				}
				return false;
			}
		}

		return false;
	}

	@Override
	public boolean broadcastTaskError(String taskId, String member, Throwable exception) {
		Set<Member> members = cluster.getMembers();
		for (Member m : members) {
			if (member.equals(m.getUuid())) {
				MemberResponse<Boolean> response = MemberTasks.executeOptimistic(communicationExecutorService, m,
						new FailedTaskOp<Throwable>(topologyName, taskId, exception));

				if (response != null && response.getValue()) {
					return true;
				}
				return false;
			}
		}
		return false;
	}

	@Override
	public boolean broadcastTaskCancellation(String taskId, String member) {
		Set<Member> members = cluster.getMembers();
		for (Member m : members) {
			if (member.equals(m.getUuid())) {
				MemberResponse<Boolean> response = MemberTasks.executeOptimistic(communicationExecutorService, m,
						new CancelReturnOp<Serializable>(topologyName, taskId));

				if (response != null && response.getValue()) {
					return true;
				}
				return false;
			}
		}

		return false;
	}

	@Override
	public Collection<HazeltaskTask<?>> getLocalPendingTasks(String predicate) {
		Set<String> keys = pendingTask.localKeySet(new SqlPredicate(predicate));
		return pendingTask.getAll(keys).values();
	}

	@Override
	public Collection<HazeltaskTask<?>> getLocalPendingTasks() {
		Set<String> keys = pendingTask.localKeySet();
		return pendingTask.getAll(keys).values();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((communicationExecutorService == null) ? 0 : communicationExecutorService.hashCode());
		result = prime * result + ((me == null) ? 0 : me.hashCode());
		result = prime * result + ((pendingTask == null) ? 0 : pendingTask.hashCode());
		result = prime * result + ((taskResponseTopic == null) ? 0 : taskResponseTopic.hashCode());
		result = prime * result + ((topologyName == null) ? 0 : topologyName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HazelcastExecutorTopologyService other = (HazelcastExecutorTopologyService) obj;
		if (communicationExecutorService == null) {
			if (other.communicationExecutorService != null)
				return false;
		} else if (!communicationExecutorService.equals(other.communicationExecutorService))
			return false;
		if (me == null) {
			if (other.me != null)
				return false;
		} else if (!me.equals(other.me))
			return false;
		if (pendingTask == null) {
			if (other.pendingTask != null)
				return false;
		} else if (!pendingTask.equals(other.pendingTask))
			return false;
		if (taskResponseTopic == null) {
			if (other.taskResponseTopic != null)
				return false;
		} else if (!taskResponseTopic.equals(other.taskResponseTopic))
			return false;
		if (topologyName == null) {
			if (other.topologyName != null)
				return false;
		} else if (!topologyName.equals(other.topologyName))
			return false;
		return true;
	}

}
