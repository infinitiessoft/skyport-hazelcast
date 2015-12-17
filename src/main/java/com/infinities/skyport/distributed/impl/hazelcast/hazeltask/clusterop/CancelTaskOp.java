package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.local.LocalTaskExecutorService;

public class CancelTaskOp extends AbstractClusterOp<Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String taskid;


	private CancelTaskOp() {
		super("");
	}

	public CancelTaskOp(String topologyName, String taskId) {
		super(topologyName);
		this.taskid = taskId;
	}

	@Override
	public Boolean call() throws Exception {
		LocalTaskExecutorService localSvc = this.getLocalTaskExecutorService();
		return localSvc.cancelTask(taskid);
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
