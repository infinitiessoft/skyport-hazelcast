package com.infinities.skyport.distributed.impl.hazelcast;

import java.util.concurrent.TimeUnit;

import com.hazelcast.core.ISemaphore;
import com.infinities.skyport.distributed.DistributedSemaphore;

/**
 * Hazelcast implementation of a distributed semaphore. Delegates to an
 * {@link com.hazelcast.core.ISemaphore}.
 * 
 * @author vanessa.williams
 */
public class HazelcastSemaphore implements DistributedSemaphore {

	private final ISemaphore delegate;


	public String getName() {
		return delegate.getName();
	}

	public HazelcastSemaphore(ISemaphore delegate) {
		this.delegate = delegate;
	}

	@Override
	public void acquire() throws InterruptedException {
		delegate.acquire();
	}

	@Override
	public boolean tryAcquire() {
		return delegate.tryAcquire();
	}

	@Override
	public boolean tryAcquire(long l, TimeUnit timeUnit) throws InterruptedException {
		return delegate.tryAcquire(l, timeUnit);
	}

	@Override
	public void release() {
		delegate.release();
	}

	@Override
	public int availablePermits() {
		return delegate.availablePermits();
	}
}
