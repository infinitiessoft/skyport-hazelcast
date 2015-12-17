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
import com.infinities.skyport.compute.entity.ComputeObject;
import com.infinities.skyport.compute.entity.NetworkAdapter;
import com.infinities.skyport.compute.entity.NetworkStatistics;

public class NetworkAdapterStreamSerializer implements StreamSerializer<NetworkAdapter> {

	@Override
	public int getTypeId() {
		return SerializationConstants.NETWORKADAPTER_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, NetworkAdapter object) throws IOException {
		// AbstractEntity
		out.writeObject(object.getId());
		out.writeUTF(object.getDesc());
		out.writeInt(object.getVersion());
		// AbstractVirtEntity
		out.writeUTF(object.getConfig());
		// NetworkAdapter
		out.writeUTF(object.getMac());
		out.writeUTF(object.getName());
		out.writeUTF(object.getIp());
		out.writeObject(object.getSpeed());
		out.writeObject(object.getLinespeed());
		out.writeUTF(object.getState());
		out.writeUTF(object.getType());
		out.writeUTF(object.getNicid());
		out.writeObject(object.getVlanid());
		out.writeUTF(object.getNetwork());
		out.writeUTF(object.getInstanceid());
		out.writeUTF(object.getInstancetype());
		out.writeObject(object.getTemplateBaseEntity());
		out.writeObject(object.getStatistics());
		out.writeUTF(object.getVmId());
		out.writeUTF(object.getConfigid());
	}

	@Override
	public NetworkAdapter read(ObjectDataInput in) throws IOException {
		NetworkAdapter ret = new NetworkAdapter();
		// AbstractEntity
		Long id = in.readObject();
		ret.setId(id);
		ret.setDesc(in.readUTF());
		ret.setVersion(in.readInt());
		// AbstractVirtEntity
		ret.setConfig(in.readUTF());
		// NetworkAdapter
		ret.setMac(in.readUTF());
		ret.setName(in.readUTF());
		ret.setIp(in.readUTF());
		Integer speed = in.readObject();
		ret.setSpeed(speed);
		Integer linespeed = in.readObject();
		ret.setLinespeed(linespeed);
		ret.setState(in.readUTF());
		ret.setType(in.readUTF());
		ret.setNicid(in.readUTF());
		Integer vlanid = in.readObject();
		ret.setVlanid(vlanid);
		ret.setNetwork(in.readUTF());
		ret.setInstanceid(in.readUTF());
		ret.setInstancetype(in.readUTF());
		ComputeObject templateBaseEntity = in.readObject();
		ret.setTemplateBaseEntity(templateBaseEntity);
		NetworkStatistics statistics = in.readObject();
		ret.setStatistics(statistics);
		ret.setVmId(in.readUTF());
		ret.setConfigid(in.readUTF());
		return ret;
	}

}
