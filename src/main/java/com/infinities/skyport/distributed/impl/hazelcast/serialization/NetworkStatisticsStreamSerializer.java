package com.infinities.skyport.distributed.impl.hazelcast.serialization;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.infinities.skyport.compute.entity.NetworkStatistics;

public class NetworkStatisticsStreamSerializer implements StreamSerializer<NetworkStatistics> {

	@Override
	public int getTypeId() {
		return SerializationConstants.NETWORKSTATISTICS_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, NetworkStatistics object) throws IOException {
		out.writeObject(object.getRxdropped());
		out.writeObject(object.getRxrate());
		out.writeObject(object.getTxdropped());
		out.writeObject(object.getTxrate());
	}

	@Override
	public NetworkStatistics read(ObjectDataInput in) throws IOException {
		NetworkStatistics ret = new NetworkStatistics();
		Double rxdropped = in.readObject();
		ret.setRxdropped(rxdropped);
		Double rxrate = in.readObject();
		ret.setRxrate(rxrate);
		Double txdropped = in.readObject();
		ret.setTxdropped(txdropped);
		Double txrate = in.readObject();
		ret.setTxrate(txrate);
		return ret;
	}

}
