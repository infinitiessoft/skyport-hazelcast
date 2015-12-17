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
