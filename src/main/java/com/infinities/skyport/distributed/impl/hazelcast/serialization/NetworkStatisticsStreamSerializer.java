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
