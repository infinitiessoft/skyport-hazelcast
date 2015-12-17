package com.infinities.skyport.distributed.impl.hazelcast;

import com.hazelcast.core.IFunction;
import com.infinities.skyport.distributed.DistributedFunction;

public class HazelcastFunction<K> implements IFunction<K, K> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private DistributedFunction<K, K> function;


	public HazelcastFunction(DistributedFunction<K, K> function) {
		this.function = function;
	}

	@Override
	public K apply(K input) {
		return function.apply(input);
	}

}
