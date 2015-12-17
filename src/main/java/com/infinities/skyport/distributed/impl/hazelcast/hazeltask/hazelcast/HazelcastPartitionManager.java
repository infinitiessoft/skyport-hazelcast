package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.hazelcast;

import java.io.Serializable;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;

public class HazelcastPartitionManager implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final Logger logger = LoggerFactory.getLogger(HazelcastPartitionManager.class);
	private final CopyOnWriteArrayList<PartitionLostListener> listeners = new CopyOnWriteArrayList<PartitionLostListener>();
	private final PartitionService partitionService;


	public HazelcastPartitionManager(PartitionService partitionService) {
		this.partitionService = partitionService;
		partitionService.addMigrationListener(new MigrationListener() {

			@Override
			public void migrationStarted(MigrationEvent migrationEvent) {
			}

			@Override
			public void migrationFailed(MigrationEvent migrationEvent) {
				migrationCompleted(migrationEvent);
			}

			@Override
			public void migrationCompleted(MigrationEvent migrationEvent) {
				// TODO how about migrationEvent.getOldOwner() != null?
				if (migrationEvent.getOldOwner() == null) {
					for (PartitionLostListener listener : listeners) {
						try {
							listener.partitionLost(migrationEvent);
						} catch (Exception e) {
							// swallow
							logger.error("An exception was thrown by our partitionLost event listener.  I will ignore it. ",
									e);
						}
					}
				}
			}
		});
	}

	public Partition getPartition(String id) {
		return partitionService.getPartition(id);
	}

	public void addPartitionListener(PartitionLostListener listener) {
		listeners.add(listener);
	}


	public interface PartitionLostListener {

		void partitionLost(MigrationEvent migrationEvent);
	}

}
