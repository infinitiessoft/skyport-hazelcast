package com.infinities.skyport.distributed.impl.hazelcast.hazeltask;

public interface ServiceListenable<S extends ServiceListenable<S>> {

	void addServiceListener(HazeltaskServiceListener<S> listener);
}
