package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor;

import com.infinities.skyport.distributed.DistributedExecutor;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.ServiceListenable;

public interface DistributedExecutorService extends ServiceListenable<DistributedExecutorService>, DistributedExecutor {

	void startup();
}
