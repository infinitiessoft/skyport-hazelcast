package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.local;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.ExecutorListener;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.IExecutorTopologyService;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.AbortableTask;

public class TasksInProgressTracker implements ExecutorListener {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(TasksInProgressTracker.class);
	private final IExecutorTopologyService topologyService;
	protected final Map<String, AbortableTask<?>> tasksInProgress = new ConcurrentHashMap<String, AbortableTask<?>>();


	public TasksInProgressTracker(IExecutorTopologyService topologyService) {
		this.topologyService = topologyService;
	}

	@Override
	public void beforeExecute(Thread thread, AbortableTask<?> task) {
		// no lock. another thread will done this.
		if (tasksInProgress.containsKey(task.getId())) {
			logger.debug("duplicate task, abort: {}", task.getId());
			task.interrupt();
			task.abort();
			return;
		}
		tasksInProgress.put(task.getId(), task);
		// no lock. another node will done this.
		if (!topologyService.getLocalPendingTaskKeys().contains(task.getId())) {
			logger.warn("task {} not in LocalPendingTask, interrupt", task.getId());
			task.interrupt();
			return;
		}
		// no lock. another node will done this.
		if (!topologyService.getPendingTasks().tryLock(task.getId())) {
			logger.warn("task {} already lock, no execute again, interrupt", task.getId());
			task.interrupt();
			return;
		}
		// get lock. alreay remove.
		if (!topologyService.getPendingTasks().containsKey(task.getId())) {
			// removed means alreay boardcast, don't execute again.
			logger.warn("task {} already removed, no execute again, abort", task.getId());
			task.abort(); // unlock
			return;
		}
	}

	@Override
	public void afterExecute(AbortableTask<?> task, Throwable exception) {
		// we finished this work... lets tell everyone about it!
		// result is null when no exeute becase already execute.

		if (task.isInterrupted() && task.isAbort()) { // no
														// unlock
			logger.debug("task {} interrupt and abort", task.getId());
			return;
		}
		try {
			logger.debug("task {} interrupt ? {}", new Object[] { task.getId(), task.isInterrupted() });
			if (task.isInterrupted()) { // no unlock
				logger.debug("task {} interrupt", task.getId());
				return;
			}
			try {
				if (task.isAbort()) { // no response, remove if exist and unlock
					return;
				}
				if (!Strings.isNullOrEmpty(task.getFromMember())) {
					boolean success = exception == null && task.getException() == null;
					boolean ret;
					if (success) {
						ret = topologyService.broadcastTaskCompletion(task.getId(), task.getFromMember(), task.getResult());
					} else {
						Throwable resolvedException = (task.getException() != null) ? task.getException() : exception;
						ret = topologyService.broadcastTaskError(task.getId(), task.getFromMember(), resolvedException);
					}
					if (!ret) {
						logger.error("An error occurred while attempting to notify members of completed task");
					}
				}
			} catch (RuntimeException e) {
				logger.error("An error occurred while attempting to notify members of completed task", e);
			} finally {
				try {
					topologyService.removePendingTask(task.getId());
				} finally {
					try {
						topologyService.getPendingTasks().unlock(task.getId());
					} catch (Exception e) {
						logger.warn("unlock problem", e);
					}
				}
			}
		} finally {
			tasksInProgress.remove(task.getId());
		}
	}

	public Collection<AbortableTask<?>> getTasksInProgress() {
		return tasksInProgress.values();
	}
}
