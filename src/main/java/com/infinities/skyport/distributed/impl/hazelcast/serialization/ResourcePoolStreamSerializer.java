package com.infinities.skyport.distributed.impl.hazelcast.serialization;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.infinities.skyport.compute.entity.ResourcePool;

public class ResourcePoolStreamSerializer implements StreamSerializer<ResourcePool> {

	@Override
	public int getTypeId() {
		return SerializationConstants.RESOURCEPOOL_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, ResourcePool object) throws IOException {
		// AbstractEntity
		out.writeObject(object.getId());
		out.writeUTF(object.getDesc());
		out.writeInt(object.getVersion());
		// AbstractVirtEntity
		out.writeUTF(object.getConfig());
		// ResourcePool
		out.writeUTF(object.getName());
		out.writeUTF(object.getResourceid());
		out.writeUTF(object.getDatacenterid());
		out.writeUTF(object.getHypervisortype());
		out.writeUTF(object.getConfigid());
	}

	@Override
	public ResourcePool read(ObjectDataInput in) throws IOException {
		ResourcePool ret = new ResourcePool();
		// AbstractEntity
		Long id = in.readObject();
		ret.setId(id);
		ret.setDesc(in.readUTF());
		ret.setVersion(in.readInt());
		// AbstractVirtEntity
		ret.setConfig(in.readUTF());
		// ResourcePool
		ret.setName(in.readUTF());
		ret.setResourceid(in.readUTF());
		ret.setDatacenterid(in.readUTF());
		ret.setHypervisortype(in.readUTF());
		ret.setConfigid(in.readUTF());
		return ret;
	}

}
