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
import com.infinities.skyport.compute.entity.Cluster;

public class ClusterStreamSerializer implements StreamSerializer<Cluster> {

	@Override
	public int getTypeId() {
		return SerializationConstants.CLUSTER_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, Cluster object) throws IOException {
		out.writeUTF(object.getName());
		out.writeUTF(object.getClusterId());
		out.writeUTF(object.getDescription());
		out.writeUTF(object.getDataCenterId());
		out.writeUTF(object.getHypervisorType());
		out.writeUTF(object.getConfig());
		out.writeUTF(object.getConfigid());
	}

	@Override
	public Cluster read(ObjectDataInput in) throws IOException {
		Cluster ret = new Cluster();
		ret.setName(in.readUTF());
		ret.setClusterId(in.readUTF());
		ret.setDescription(in.readUTF());
		ret.setDataCenterId(in.readUTF());
		ret.setHypervisorType(in.readUTF());
		ret.setConfig(in.readUTF());
		ret.setConfigid(in.readUTF());
		return ret;
	}

}
