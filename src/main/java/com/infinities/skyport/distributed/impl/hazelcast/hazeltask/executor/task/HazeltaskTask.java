package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task;

import com.infinities.skyport.async.ServiceProviderAware;

public interface HazeltaskTask<T> extends Task<T>, ServiceProviderAware {

	boolean isStarted();

	void setStarted(boolean b);

	int getSubmissionCount();

	void setSubmissionCount(int count);

	void setException(Exception e);

}
