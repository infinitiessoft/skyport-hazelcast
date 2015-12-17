package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.DistributedFutureTracker;

public class CompletedTaskOp<R> extends AbstractClusterOp<Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String taskid;
	private R response;


	private CompletedTaskOp() {
		super("");
	}

	public CompletedTaskOp(String topologyName, String taskId, R response) {
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

	public R getResponse() {
		return response;
	}

}
