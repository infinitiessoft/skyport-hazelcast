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
