package com.infinities.skyport.distributed.impl.hazelcast.serialization;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.infinities.skyport.compute.entity.Network;

public class NetworkStreamSerializer implements StreamSerializer<Network> {

	@Override
	public int getTypeId() {
		return SerializationConstants.NETWORK_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, Network object) throws IOException {
		// AbstractEntity
		out.writeObject(object.getId());
		out.writeUTF(object.getDesc());
		out.writeInt(object.getVersion());
		// AbstractVirtEntity
		out.writeUTF(object.getConfig());
		// Network
		out.writeUTF(object.getNetworkid());
		out.writeUTF(object.getResourceid());
		out.writeUTF(object.getName());
		out.writeUTF(object.getStatus());
		out.writeUTF(object.getDatacenterid());
		out.writeObject(object.getIsdisplay());
		out.writeUTF(object.getVmId());
		out.writeUTF(object.getConfigid());
	}

	@Override
	public Network read(ObjectDataInput in) throws IOException {
		Network ret = new Network();
		// AbstractEntity
		Long id = in.readObject();
		ret.setId(id);
		ret.setDesc(in.readUTF());
		ret.setVersion(in.readInt());
		// AbstractVirtEntity
		ret.setConfig(in.readUTF());
		// Network
		ret.setNetworkid(in.readUTF());
		ret.setResourceid(in.readUTF());
		ret.setName(in.readUTF());
		ret.setStatus(in.readUTF());
		ret.setDatacenterid(in.readUTF());
		Boolean isdisplay = in.readObject();
		ret.setIsdisplay(isdisplay);
		ret.setVmId(in.readUTF());
		ret.setConfigid(in.readUTF());
		return ret;
	}

}
