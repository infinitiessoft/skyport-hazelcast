package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.local;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.infinities.skyport.ServiceProvider;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.NamedThreadFactory;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.collections.tracked.ITrackedQueue;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.IExecutorTopologyService;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.AbortableTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.Task;
import com.infinities.skyport.model.PoolConfig;

public class LocalTaskExecutorService {

	private final Logger logger = LoggerFactory.getLogger(LocalTaskExecutorService.class);
	private final HazeltaskThreadPoolExecutor localExecutorPool;
	private final ITrackedQueue<Task<?>> taskQueue;
	protected TasksInProgressTracker tasksInProgressTracker;
	private final IExecutorTopologyService topologyService;
	private final ServiceProvider serviceProvider;


	public LocalTaskExecutorService(NamedThreadFactory namedThreadFactory, IExecutorTopologyService topologyService,
			PoolConfig poolConfig, ServiceProvider serviceProvider) {
		this.topologyService = topologyService;
		this.serviceProvider = serviceProvider;
		taskQueue = new TrackedPriorityBlockingQueue<Task<?>>();
		@SuppressWarnings({ "unchecked", "rawtypes" })
		BlockingQueue<Runnable> blockingQueue = (BlockingQueue) taskQueue;
		localExecutorPool = new HazeltaskThreadPoolExecutor(poolConfig.getCoreSize(), poolConfig.getMaxSize(),
				poolConfig.getKeepAlive(), TimeUnit.MILLISECONDS, blockingQueue, namedThreadFactory.named("worker"),
				new AbortPolicy());
		tasksInProgressTracker = new TasksInProgressTracker(topologyService);
		localExecutorPool.addListener(tasksInProgressTracker);
	}

	public long getQueueSize() {
		return taskQueue.size();
	}

	public <T> void execute(final HazeltaskTask<T> command) throws RejectedExecutionException {
		if (localExecutorPool.isShutdown()) {
			logger.warn("Cannot enqueue the task: {}.  The executor threads are shutdown.", command);
			return;
		}
		command.setServiceProvider(serviceProvider);
		localExecutorPool.execute(new HazeltaskTaskWrapper<T>(command));
	}

	public void shutdown() {
		localExecutorPool.shutdown();
	}

	public List<? extends Runnable> shutdownNow() {
		return localExecutorPool.shutdownNow();
	}

	public boolean isShutdown() {
		return localExecutorPool.isShutdown();
	}

	public void setCorePoolSize(int corePoolSize) {
		localExecutorPool.setCorePoolSize(corePoolSize);
	}

	public void setKeepAliveTime(int time, TimeUnit unit) {
		localExecutorPool.setKeepAliveTime(time, unit);
	}

	public void setMaximumPoolSize(int maximumPoolSize) {
		localExecutorPool.setMaximumPoolSize(maximumPoolSize);
	}

	public Boolean cancelTask(String taskId) {
		ITrackedQueue<Task<?>> queue = this.taskQueue;
		if (queue != null) {
			Iterator<Task<?>> it = queue.iterator();
			while (it.hasNext()) {
				Task<?> task = it.next();
				if (task.getId().equals(taskId)) {
					topologyService.broadcastTaskCancellation(taskId, task.getFromMember());
					it.remove();
					return true;
				}
			}
		}
		// TODO: allow cancelling of inprogress tasks but we need access to the
		// Thread that is running it
		return false;
	}

	public int getActiveCount() {
		return localExecutorPool.getActiveCount();
	}

	public int getMaximumPoolSize() {
		return localExecutorPool.getMaximumPoolSize();
	}

	public int getCorePoolSize() {
		return localExecutorPool.getCorePoolSize();
	}

	public ITrackedQueue<Task<?>> getQueue() {
		return taskQueue;
	}

	public Collection<AbortableTask<?>> getTasksInProcess() {
		return tasksInProgressTracker.getTasksInProgress();
	}

	public <T> boolean contains(Task<T> task) {
		return this.taskQueue.contains(task) || tasksInProgressTracker.getTasksInProgress().contains(task);
	}


	public class HazeltaskTaskWrapper<T> implements AbortableTask<T> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final HazeltaskTask<T> task;
		private boolean abort;
		private boolean interrupt;


		private HazeltaskTaskWrapper(HazeltaskTask<T> task) {
			this.task = task;
		}

		public HazeltaskTask<T> getHazeltaskTask() {
			return task;
		}

		// interrupted = ignore totally
		// abort = only remove
		@Override
		public void run() {
			logger.debug("start task {}, is interrupt? {}", new Object[] { task.getId(), isInterrupted() });
			if (isInterrupted()) {
				logger.debug("task {} interrupt", task.getId());
				return;
			}
			if (!task.isStarted()) { // task always in queue
				task.setStarted(true); // task started
				topologyService.getPendingTasks().set(task.getId(), task);
				task.run();
				// task.setDone(true); // task done
				// topologyService.getPendingTasks().set(task.getId(), task);
			} else {
				// if (task.isDone()) { // done but not boardcast yet
				// logger.warn("task already done but not remove from pending task list: {}",
				// task.getId());
				// throw new
				// RuntimeException("task might be already done but interrupt by partition migrating");
				// } else {
				logger.warn("task is interrupted in the middle stage, restart the task can be dangerous: {}", task.getId());
				task.setException(new RuntimeException("task is interrupted because of uncertainty issue"));
				// }
			}
		}

		@Override
		public long getTimeCreated() {
			return task.getTimeCreated();
		}

		@Override
		public String getId() {
			return task.getId();
		}

		@Override
		public int hashCode() {
			return task.hashCode();
		}

		@Override
		public T getResult() {
			return task.getResult();
		}

		@Override
		public Exception getException() {
			return task.getException();
		}

		@Override
		public boolean isAbort() {
			return abort;
		}

		@Override
		public String getFromMember() {
			return task.getFromMember();
		}

		@Override
		public void abort() {
			abort = true;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!(obj instanceof Task))
				return false;
			Task<?> other = (Task<?>) obj;
			if (!this.getId().equals(other.getId())) {
				return false;
			}
			return true;
		}

		@Override
		public boolean isInterrupted() {
			return interrupt;
		}

		@Override
		public void interrupt() {
			interrupt = true;
		}

		@Override
		public void setServiceProvider(ServiceProvider serviceProvider) {
			task.setServiceProvider(serviceProvider);
		}

		@Override
		public boolean isStarted() {
			return task.isStarted();
		}

		@Override
		public void setStarted(boolean b) {
			task.setStarted(b);
		}

		@Override
		public int getSubmissionCount() {
			return task.getSubmissionCount();
		}

		@Override
		public void setSubmissionCount(int count) {
			task.setSubmissionCount(count);
		}

		@Override
		public void setException(Exception e) {
			task.setException(e);
		}

	}
}
