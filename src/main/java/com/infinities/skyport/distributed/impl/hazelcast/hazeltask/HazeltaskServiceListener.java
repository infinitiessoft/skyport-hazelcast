package com.infinities.skyport.distributed.impl.hazelcast.hazeltask;

public class HazeltaskServiceListener<T extends ServiceListenable<T>> {

	public void onBeginStart(T svc) {
	};

	public void onEndStart(T svc) {
	};

	public void onBeginShutdown(T svc) {
	};

	public void onEndShutdown(T svc) {
	};
}
