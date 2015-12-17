package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.Callable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.Hazeltask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.HazeltaskInstance;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.DistributedFutureTracker;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.local.LocalTaskExecutorService;

public abstract class AbstractClusterOp<T> implements Callable<T>, DataSerializable, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String topologyName;


	public AbstractClusterOp(String topologyName) {
		this.setTopologyName(topologyName);
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(topologyName);
		writeChildData(out);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		topologyName = in.readUTF();
		readChildData(in);
	}

	public String getTopologyName() {
		return topologyName;
	}

	public void setTopologyName(String topologyName) {
		this.topologyName = topologyName;
	}

	protected LocalTaskExecutorService getLocalTaskExecutorService() {
		HazeltaskInstance ht = Hazeltask.getInstanceByName(topologyName);
		if (ht != null) {
			return ht.getLocalExecutorService();
		}
		throw new IllegalStateException("Hazeltask was null for topology: " + topologyName);
	}

	protected DistributedFutureTracker getDistributedFutureTracker() {
		HazeltaskInstance ht = Hazeltask.getInstanceByName(topologyName);
		if (ht != null) {
			return ht.getFutureTracker();
		}
		throw new IllegalStateException("Hazeltask was null for topology: " + topologyName);
	}

	protected abstract void readChildData(ObjectDataInput in) throws IOException;

	protected abstract void writeChildData(ObjectDataOutput out) throws IOException;

}
