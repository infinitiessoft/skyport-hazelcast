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
