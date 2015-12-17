package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop;

import java.io.IOException;
import java.io.Serializable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.DistributedFutureTracker;

public class FailedTaskOp<R extends Serializable> extends AbstractClusterOp<Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String taskid;
	private Throwable response;


	private FailedTaskOp() {
		super("");
	}

	public FailedTaskOp(String topologyName, String taskId, Throwable response) {
		super(topologyName);
		this.taskid = taskId;
		this.response = response;
	}

	@Override
	public Boolean call() throws Exception {
		DistributedFutureTracker tracker = this.getDistributedFutureTracker();
		return tracker.completeTask(taskid, response);
	}

	@Override
	protected void readChildData(ObjectDataInput in) throws IOException {
		taskid = in.readUTF();
		response = in.readObject();
	}

	@Override
	protected void writeChildData(ObjectDataOutput out) throws IOException {
		out.writeUTF(taskid);
		out.writeObject(response);
	}

	public Throwable getResponse() {
		return response;
	}

}
