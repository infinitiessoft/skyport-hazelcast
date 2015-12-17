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
package com.infinities.skyport.distributed.impl.hazelcast.hazeltask;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.infinities.skyport.ServiceProvider;
import com.infinities.skyport.distributed.DistributedExecutor;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.NamedThreadFactory;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.collections.tracked.TimeComparator;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.DistributedExecutorService;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.DistributedExecutorServiceImpl;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.DistributedFutureTracker;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.HazelcastExecutorTopologyService;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.IExecutorTopologyService;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.local.LocalTaskExecutorService;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.hazelcast.HazelcastPartitionManager;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.hazelcast.HazelcastPartitionManager.PartitionLostListener;
import com.infinities.skyport.model.PoolConfig;

public class HazeltaskInstance {

	private static final Logger logger = LoggerFactory.getLogger(HazeltaskInstance.class);
	private final DistributedExecutorServiceImpl executor;
	private final static long INITIAL_TIME = 10;
	private final static long RECOVERY_TIME = 30;
	// private final ITopologyService topologyService;
	// private final HazelcastTopology topology;
	private final IExecutorTopologyService executorTopologyService;
	private final LocalTaskExecutorService localExecutorService;
	private final DistributedFutureTracker futureTracker;
	private final long maximumFutureWaitTime = TimeUnit.MINUTES.toMillis(60);
	private final TimeUnit timeUnit = TimeUnit.MINUTES;
	// private final NamedThreadFactory threadFactory;
	private final String topologyName;
	private final HazelcastInstance hazelcast;
	private final ScheduledExecutorService scheduler;
	private volatile boolean isShutdown = false;


	protected HazeltaskInstance(String topologyName, HazelcastInstance hazelcast, PoolConfig poolConfig,
			ScheduledExecutorService scheduler, ServiceProvider serviceProvider) {
		this.scheduler = scheduler;
		this.topologyName = topologyName;
		this.hazelcast = hazelcast;
		// this.topologyService = new HazelcastTopologyService(topologyName,
		// hazelcast);
		PartitionService partitionService = hazelcast.getPartitionService();
		// this.topology = new HazelcastTopology(topologyName,
		// hazelcast.getCluster().getLocalMember());

		this.executorTopologyService = new HazelcastExecutorTopologyService(topologyName, hazelcast);
		this.localExecutorService = new LocalTaskExecutorService(new NamedThreadFactory("Hazeltask", topologyName),
				executorTopologyService, poolConfig, serviceProvider);

		final DistributedFutureTracker futureTracker = new DistributedFutureTracker(executorTopologyService,
				maximumFutureWaitTime, timeUnit);
		this.futureTracker = futureTracker;
		final HazelcastPartitionManager partitionManager = new HazelcastPartitionManager(partitionService);

		partitionManager.addPartitionListener(new PartitionLostListener() {

			@Override
			public void partitionLost(MigrationEvent migrationEvent) {
				// TODO: make this faster, too many loops
				Set<String> uuids = futureTracker.getTrackedTaskIds();
				for (String uuid : uuids) {
					Partition partition = partitionManager.getPartition(uuid);
					if (migrationEvent.getPartitionId() == partition.getPartitionId()) {
						futureTracker.errorFuture(uuid, new MemberLeftException());
					}
				}
			}
		});

		executorTopologyService.addTaskResponseMessageHandler(futureTracker);

		// we don't need route feature so topology is ignore here
		executor = new DistributedExecutorServiceImpl(topologyName, executorTopologyService, futureTracker,
				localExecutorService);
	}

	protected void start() {
		setupDistributedExecutor(hazelcast, executor, executorTopologyService, localExecutorService);

		// if autoStart... we need to start
		final LifecycleService lifecycleService = hazelcast.getLifecycleService();
		LifecycleListener autoStartListener = new LifecycleListener() {

			@Override
			public void stateChanged(LifecycleEvent event) {
				if (event.getState() == LifecycleState.STARTED) {
					logger.info("{} Hazeltask instance is starting up due to Hazelcast startup", topologyName);
					executor.startup();
				} else if (event.getState() == LifecycleState.SHUTTING_DOWN) {
					logger.info("{} Hazeltask instance is shutting down due to Hazelcast shutdown", topologyName);
					executor.shutdown();
					shutdown();
				}
			}
		};
		lifecycleService.addLifecycleListener(autoStartListener);

		if (lifecycleService.isRunning()) {
			logger.info("{} Hazeltask instance is starting up", topologyName);
			executor.startup();
		}
	}

	private void setupDistributedExecutor(final HazelcastInstance hazelcast, DistributedExecutorServiceImpl svc,
			final IExecutorTopologyService executorTopologyService, final LocalTaskExecutorService localExecutorService) {

		svc.addServiceListener(new HazeltaskServiceListener<DistributedExecutorService>() {

			@Override
			public void onEndStart(DistributedExecutorService svc) {
				logger.info("{} Hazeltask instance is scheduling periodic timer tasks", topologyName);
				reschduleRecoveryTask(INITIAL_TIME);
			}

			@Override
			public void onBeginShutdown(DistributedExecutorService svc) {
				logger.info("{} Hazeltask instance is unscheduling timer tasks and stopping the timer thread", topologyName);
				isShutdown = true;
			}
		});

		executorTopologyService.addLocalEntryListener(new EntryListener<String, HazeltaskTask<?>>() {

			@Override
			public void entryAdded(EntryEvent<String, HazeltaskTask<?>> event) {
				try {
					localExecutorService.execute(event.getValue());
				} catch (RejectedExecutionException e) {
					logger.error("task is rejected", e);
				}
			}

			@Override
			public void entryRemoved(EntryEvent<String, HazeltaskTask<?>> event) {

			}

			@Override
			public void entryUpdated(EntryEvent<String, HazeltaskTask<?>> event) {

			}

			@Override
			public void entryEvicted(EntryEvent<String, HazeltaskTask<?>> event) {

			}
		});
	}

	public void shutdown() {
		logger.debug("shutdown {} HazeltaskInstance", topologyName);
		isShutdown = true;
		executor.shutdown(); // DistributedExecutorService and
								// LocalExecutorService
		futureTracker.shutdown();
		Hazeltask.instances.remove(getTopologyName());
	}

	public String getTopologyName() {
		return topologyName;
	}

	public DistributedExecutor getExecutorService() {
		return executor;
	}

	public LocalTaskExecutorService getLocalExecutorService() {
		return localExecutorService;
	}

	public DistributedFutureTracker getFutureTracker() {
		return futureTracker;
	}

	private void processPendingTask(IExecutorTopologyService executorTopologyService,
			LocalTaskExecutorService localExecutorService) {
		Collection<HazeltaskTask<?>> localPendingTask = executorTopologyService.getLocalPendingTasks();
		if (localPendingTask.size() <= 0) {
			logger.debug("{} no pending task", topologyName);
			return;
		}
		PriorityQueue<HazeltaskTask<?>> queue = new PriorityQueue<HazeltaskTask<?>>(localPendingTask.size(),
				new TimeComparator<HazeltaskTask<?>>());
		logger.debug("{} local pending task: {}", new Object[] { topologyName, localPendingTask });
		for (HazeltaskTask<?> task : localPendingTask) {
			logger.debug("{} task: {}, in process? {}", new Object[] { topologyName, task.getId().toString(),
					localExecutorService.contains(task) });
			if (!localExecutorService.contains(task)) {
				try {
					queue.offer(task);
				} catch (Throwable e) {
					logger.error("LocalExecutorService execute task failed", e);
				}
			}
		}

		logger.debug("{} Task {} unprocessed", new Object[] { topologyName, queue.size() });
		HazeltaskTask<?> task;
		while ((task = queue.poll()) != null) {
			try {
				localExecutorService.execute(task);
				logger.debug("{} task {} unprocessed, executing", new Object[] { topologyName, task.getId() });
			} catch (Throwable e) {
				logger.error("LocalExecutorService execute task failed", e);
			}
		}
	}

	private void reschduleRecoveryTask(long time) {
		if (!isShutdown) {
			scheduler.schedule(new RecoveryTask(), time, TimeUnit.SECONDS);
		}
	}


	private class RecoveryTask implements Runnable {

		@Override
		public void run() {
			Thread t = new Thread(new ThreadGroup(topologyName), new Runnable() {

				@Override
				public void run() {
					try {
						processPendingTask(executorTopologyService, localExecutorService);
					} catch (Exception e) {
						logger.warn("process pending task failed", e);
					} finally {
						reschduleRecoveryTask(RECOVERY_TIME);
					}
				}

			}, "RecoveryThread");
			t.start();
		}
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hazelcast == null) ? 0 : hazelcast.hashCode());
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
		HazeltaskInstance other = (HazeltaskInstance) obj;
		if (hazelcast == null) {
			if (other.hazelcast != null)
				return false;
		} else if (!hazelcast.equals(other.hazelcast))
			return false;
		if (topologyName == null) {
			if (other.topologyName != null)
				return false;
		} else if (!topologyName.equals(other.topologyName))
			return false;
		return true;
	}

}
