package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop;

import java.io.IOException;
import java.io.Serializable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.DistributedFutureTracker;

public class CancelReturnOp<R extends Serializable> extends AbstractClusterOp<Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String taskid;


	private CancelReturnOp() {
		super("");
	}

	public CancelReturnOp(String topologyName, String taskId) {
		super(topologyName);
		this.taskid = taskId;
	}

	@Override
	public Boolean call() throws Exception {
		DistributedFutureTracker tracker = this.getDistributedFutureTracker();
		return tracker.cancelTask(taskid);
	}

	@Override
	protected void readChildData(ObjectDataInput in) throws IOException {
		taskid = in.readUTF();
	}

	@Override
	protected void writeChildData(ObjectDataOutput out) throws IOException {
		out.writeUTF(taskid);
	}

}
