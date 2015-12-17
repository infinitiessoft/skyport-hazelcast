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

import java.io.IOException;
import java.util.concurrent.Callable;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.infinities.skyport.ServiceProvider;
import com.infinities.skyport.async.AsyncTask;

public class HazeltaskTaskImpl<T> implements HazeltaskTask<T>, DataSerializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private AsyncTask<T> task;
	private Callable<T> callable;
	private String id;
	private int submissionCount;
	private boolean isStarted;
	protected transient volatile T result;
	protected transient volatile Exception exception;
	private String fromMember;
	private long createdAtMillis;


	public HazeltaskTaskImpl() {

	}

	public HazeltaskTaskImpl(String id, String fromMember, AsyncTask<T> task) {
		this.task = task;
		this.id = id;
		this.setFromMember(fromMember);
		this.submissionCount = 1;
	}

	public HazeltaskTaskImpl(String id, String fromMember, Callable<T> task) {
		this.callable = task;
		this.id = id;
		this.setFromMember(fromMember);
		this.submissionCount = 1;
	}

	@Override
	public void run() {
		try {
			if (task != null) {
				this.result = task.call();
			} else {
				this.result = callable.call();
			}
		} catch (Exception e) {
			this.exception = e;
		}

	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public int getSubmissionCount() {
		return submissionCount;
	}

	@Override
	public void setSubmissionCount(int submissionCount) {
		this.submissionCount = submissionCount;
	}

	@Override
	public T getResult() {
		return result;
	}

	@Override
	public Exception getException() {
		return exception;
	}

	public void setResult(T result) {
		this.result = result;
	}

	@Override
	public long getTimeCreated() {
		return createdAtMillis;
	}

	@Override
	public String toString() {
		return "HazelcastTask [id=" + id + ", submissionCount=" + submissionCount + ", createdAtMillis=" + createdAtMillis
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (createdAtMillis ^ (createdAtMillis >>> 32));
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + (isStarted ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof Task))
			return false;
		Task<?> other = (Task<?>) obj;
		if (!this.getId().equals(other.getId())) {
			return false;
		}
		return true;
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeObject(task);
		out.writeLong(createdAtMillis);
		out.writeInt(submissionCount);
		out.writeBoolean(isStarted);
		out.writeUTF(fromMember);
		out.writeObject(callable);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readData(ObjectDataInput in) throws IOException {
		id = in.readUTF();
		task = (AsyncTask<T>) in.readObject();
		createdAtMillis = in.readLong();
		submissionCount = in.readInt();
		isStarted = in.readBoolean();
		fromMember = in.readUTF();
		callable = in.readObject();
	}

	@Override
	public boolean isStarted() {
		return isStarted;
	}

	@Override
	public void setStarted(boolean isStarted) {
		this.isStarted = isStarted;
	}

	@Override
	public String getFromMember() {
		return fromMember;
	}

	public void setFromMember(String fromMember) {
		this.fromMember = fromMember;
	}

	@Override
	public void setException(Exception e) {
		this.exception = e;
	}

	@Override
	public void setServiceProvider(ServiceProvider serviceProvider) {
		if (task != null) {
			task.setServiceProvider(serviceProvider);
		}
	}

}
