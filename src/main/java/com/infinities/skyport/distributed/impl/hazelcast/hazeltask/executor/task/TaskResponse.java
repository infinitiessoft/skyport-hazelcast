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
package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task;

import java.io.Serializable;

import com.hazelcast.core.Member;

public class TaskResponse<R> implements Serializable {

	private static final long serialVersionUID = 1L;
	private final String from;
	private final String taskId;
	private final R response;
	private final Throwable error;
	private final Status status;


	public static enum Status implements Serializable {
		SUCCESS, FAILURE, CANCELLED
	}


	public TaskResponse(Member from, String taskId, R response, Status status) {
		this.from = from.getUuid();
		this.taskId = taskId;
		this.response = response;
		this.error = null;
		this.status = status;
	}

	public TaskResponse(Member from, String taskId, Throwable error) {
		this.from = from.getUuid();
		this.taskId = taskId;
		this.error = error;
		this.response = null;
		this.status = Status.FAILURE;
	}

	public String getFrom() {
		return from;
	}

	public String getTaskId() {
		return taskId;
	}

	public R getResponse() {
		return response;
	}

	public Throwable getError() {
		return error;
	}

	public Status getStatus() {
		return status;
	}
}
