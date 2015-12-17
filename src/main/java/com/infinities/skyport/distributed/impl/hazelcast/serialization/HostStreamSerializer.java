package com.infinities.skyport.distributed.impl.hazelcast.serialization;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.infinities.skyport.compute.entity.Host;
import com.infinities.skyport.compute.entity.Statistics;

public class HostStreamSerializer implements StreamSerializer<Host> {

	@Override
	public int getTypeId() {
		return SerializationConstants.HOST_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, Host object) throws IOException {
		// AbstractEntity
		out.writeObject(object.getId());
		out.writeUTF(object.getDesc());
		out.writeInt(object.getVersion());
		// AbstractVirtEntity
		out.writeUTF(object.getConfig());
		// Host
		out.writeUTF(object.getHostid());
		out.writeUTF(object.getIp());
		out.writeUTF(object.getName());
		out.writeUTF(object.getStatus());
		out.writeObject(object.getPort());
		out.writeObject(object.getStatistics());
		out.writeUTF(object.getConfigid());
	}

	@Override
	public Host read(ObjectDataInput in) throws IOException {
		Host ret = new Host();
		// AbstractEntity
		Long id = in.readObject();
		ret.setId(id);
		ret.setDesc(in.readUTF());
		ret.setVersion(in.readInt());
		// AbstractVirtEntity
		ret.setConfig(in.readUTF());
		// Host
		ret.setHostid(in.readUTF());
		ret.setIp(in.readUTF());
		ret.setName(in.readUTF());
		ret.setStatus(in.readUTF());
		Integer port = in.readObject();
		ret.setPort(port);
		Statistics statistics = in.readObject();
		ret.setStatistics(statistics);
		ret.setConfigid(in.readUTF());
		return ret;
	}

}
