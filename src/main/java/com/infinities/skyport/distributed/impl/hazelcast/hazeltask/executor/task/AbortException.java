package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task;

public class AbortException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public AbortException() {
	}

	public AbortException(String message) {
		super(message);
	}

	public AbortException(Throwable cause) {
		super(cause);
	}

	public AbortException(String message, Throwable cause) {
		super(message, cause);
	}

}
