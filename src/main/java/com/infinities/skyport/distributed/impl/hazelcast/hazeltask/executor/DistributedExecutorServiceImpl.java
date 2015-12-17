package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.infinities.skyport.async.AsyncTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.HazeltaskServiceListener;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.local.LocalTaskExecutorService;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTaskImpl;

public class DistributedExecutorServiceImpl implements DistributedExecutorService {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(DistributedExecutorServiceImpl.class);
	private static final int AWAIT_TERMINATION_POLL_INTERVAL = 100;
	private final LocalTaskExecutorService localExecutorService;
	private final DistributedFutureTracker futureTracker;
	protected CopyOnWriteArrayList<HazeltaskServiceListener<DistributedExecutorService>> listeners = new CopyOnWriteArrayList<HazeltaskServiceListener<DistributedExecutorService>>();
	private final IExecutorTopologyService topologyService;
	private boolean isStarted;
	private boolean isShutdown;
	private final String topologyName;


	public DistributedExecutorServiceImpl(String topologyName, IExecutorTopologyService topologyService,
			DistributedFutureTracker futureTracker, LocalTaskExecutorService localExecutorService) {
		this.topologyName = topologyName;
		this.topologyService = topologyService;
		this.futureTracker = futureTracker;
		this.localExecutorService = localExecutorService;
	}

	@Override
	public void shutdown() {
		doShutdownNow(false);
	}

	@Override
	public List<Runnable> shutdownNow() {
		return new ArrayList<Runnable>(doShutdownNow(true));
	}

	@SuppressWarnings("unchecked")
	protected synchronized List<HazeltaskTask<?>> doShutdownNow(boolean shutdownNow) {
		if (!isShutdown) {
			isShutdown = true;
			for (HazeltaskServiceListener<DistributedExecutorService> listener : listeners) {
				listener.onBeginShutdown(this);
			}
			// TODO: is everything shutdown?
			List<HazeltaskTask<?>> tasks = Collections.emptyList();
			if (shutdownNow) {
				tasks = (List<HazeltaskTask<?>>) this.localExecutorService.shutdownNow();
			} else {
				this.localExecutorService.shutdown();
			}

			for (HazeltaskServiceListener<DistributedExecutorService> listener : listeners) {
				listener.onEndShutdown(this);
			}
			listeners.clear();

			return tasks;
		}
		return Collections.emptyList();
	}

	@Override
	public boolean isShutdown() {
		return isShutdown;
	}

	@Override
	public boolean isTerminated() {
		return isShutdown && (futureTracker == null || futureTracker.size() == 0);
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {

		if (!isShutdown) {
			throw new IllegalStateException("shutdown() must be called before awaitTermination()");
		}

		if (futureTracker == null) {
			throw new IllegalStateException("FutureTracker is null");
		}

		long elapsed = 0;
		long ms = unit.toMillis(timeout);
		long interval = Math.min(AWAIT_TERMINATION_POLL_INTERVAL, ms);

		do {
			if (futureTracker.size() == 0) {
				return true;
			}

			Thread.sleep(interval);
			elapsed += interval;

		} while (elapsed < ms);

		return false;
	}

	@Override
	public <T> ListenableFuture<T> submit(Callable<T> task) {
		HazeltaskTask<T> taskWrapper = createTaskWrapper(task);
		DistributedFuture<T> future = futureTracker.createFuture(taskWrapper);
		if (!submitHazelcastTask(taskWrapper, false)) {
			logger.error("Unable to submit HazelcastTask to worker member");
			future.setCancelled(false);
			futureTracker.remove(taskWrapper.getId());
		}
		return future;
	}

	@Override
	public <T> ListenableFuture<T> submit(AsyncTask<T> task) {
		HazeltaskTask<T> taskWrapper = createTaskWrapper(task);
		DistributedFuture<T> future = futureTracker.createFuture(taskWrapper);
		if (!submitHazelcastTask(taskWrapper, false)) {
			logger.error("Unable to submit HazelcastTask to worker member");
			future.setCancelled(false);
			futureTracker.remove(taskWrapper.getId());
		}
		return future;
	}

	private void validateTask(Object task) {
		if (!(task instanceof Serializable)) {
			throw new IllegalArgumentException("The task type " + task.getClass() + " must implement Serializable");
		}
	}

	private <T> HazeltaskTask<T> createTaskWrapper(Callable<T> task) {
		validateTask(task);
		return new HazeltaskTaskImpl<T>(UUID.randomUUID().toString(), topologyService.getLocalMember().getUuid(), task);
	}

	private <T> HazeltaskTask<T> createTaskWrapper(AsyncTask<T> task) {
		validateTask(task);
		return new HazeltaskTaskImpl<T>(UUID.randomUUID().toString(), topologyService.getLocalMember().getUuid(), task);
	}

	@Override
	public ListenableFuture<?> submit(Runnable task) {
		throw new RuntimeException("Not Implemented Yet");
	}

	@Override
	public <T> ListenableFuture<T> submit(Runnable task, T result) {
		throw new RuntimeException("Not Implemented Yet");
	}

	@Override
	public void execute(Runnable arg0) {
		throw new RuntimeException("Not Implemented Yet");
	}

	private <T> boolean submitHazelcastTask(HazeltaskTask<T> wrapper, boolean isResubmitting) {

		// WorkId workKey = wrapper.getWorkId();
		/*
		 * with acknowledgeWorkSubmition, we will sit in this loop until a node
		 * accepts our work item. Currently, a node will accept as long as a
		 * MemberLeftException is not thrown
		 */
		if (isResubmitting) {
			wrapper.setSubmissionCount(wrapper.getSubmissionCount() + 1);
			return topologyService.addPendingTask(wrapper, true);
		} else {
			return topologyService.addPendingTask(wrapper, false);
		}
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		throw new RuntimeException("Not Implemented Yet");
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException {
		throw new RuntimeException("Not Implemented Yet");
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		throw new RuntimeException("Not Implemented Yet");
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		throw new RuntimeException("Not Implemented Yet");
	}

	@Override
	public synchronized void startup() {
		if (!isStarted) {
			for (HazeltaskServiceListener<DistributedExecutorService> listener : listeners) {
				listener.onBeginStart(this);
			}
			// TODO: clean up this code. There isn't really a startup process
			// anymore
			for (HazeltaskServiceListener<DistributedExecutorService> listener : listeners) {
				listener.onEndStart(this);
			}
			isStarted = true;
		}
	}

	@Override
	public void addServiceListener(HazeltaskServiceListener<DistributedExecutorService> listener) {
		this.listeners.add(listener);
	}

	public LocalTaskExecutorService getLocalTaskExecutorService() {
		return this.localExecutorService;
	}

	public DistributedFutureTracker getDistributedFutureTracker() {
		return this.futureTracker;
	}

	@Override
	public void destroy() {
		shutdown();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((topologyService == null) ? 0 : topologyService.hashCode());
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
		DistributedExecutorServiceImpl other = (DistributedExecutorServiceImpl) obj;
		if (topologyService == null) {
			if (other.topologyService != null)
				return false;
		} else if (!topologyService.equals(other.topologyService))
			return false;
		return true;
	}

	@Override
	public String getName() {
		return topologyName;
	}

	@Override
	public void setCorePoolSize(int corePoolSize) {
		localExecutorService.setCorePoolSize(corePoolSize);
	}

	@Override
	public void setKeepAliveTime(int time, TimeUnit unit) {
		localExecutorService.setKeepAliveTime(time, unit);
	}

	@Override
	public void setMaximumPoolSize(int maximumPoolSize) {
		localExecutorService.setMaximumPoolSize(maximumPoolSize);
	}

}
