package com.infinities.skyport.distributed.impl.hazelcast.serialization;

import java.io.IOException;
import java.util.Date;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.infinities.skyport.compute.entity.ComputeObject;
import com.infinities.skyport.compute.entity.Disks;
import com.infinities.skyport.compute.entity.NetworkAdapters;

public class ComputeObjectStreamSerializer implements StreamSerializer<ComputeObject> {

	@Override
	public int getTypeId() {
		return SerializationConstants.COMPUTEOBJECT_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, ComputeObject object) throws IOException {
		// AbstractEntity
		out.writeObject(object.getId());
		out.writeUTF(object.getDesc());
		out.writeInt(object.getVersion());
		// AbstractVirtEntity
		out.writeUTF(object.getConfig());
		// ComputeObject
		out.writeObject(object.getCpunum());
		out.writeObject(object.getMemorysize());
		out.writeObject(object.getCreationdate());
		out.writeObject(object.getDisks());
		out.writeObject(object.getNics());
		out.writeUTF(object.getName());
		out.writeUTF(object.getResourceid());
		out.writeUTF(object.getTemplateid());
		out.writeUTF(object.getOs());
		out.writeUTF(object.getVmtype());
		out.writeUTF(object.getHypervisortype());
		out.writeUTF(object.getStatus());
		out.writeUTF(object.getTimeZone());
		out.writeUTF(object.getConfigid());
	}

	@Override
	public ComputeObject read(ObjectDataInput in) throws IOException {
		ComputeObject ret = new ComputeObject();
		// AbstractEntity
		Long id = in.readObject();
		ret.setId(id);
		ret.setDesc(in.readUTF());
		ret.setVersion(in.readInt());
		// AbstractVirtEntity
		ret.setConfig(in.readUTF());
		// ComputeObject
		Integer cpunum = in.readObject();
		ret.setCpunum(cpunum);
		Long memorysize = in.readObject();
		ret.setMemorysize(memorysize);
		Date creationdate = in.readObject();
		ret.setCreationdate(creationdate);
		Disks disks = in.readObject();
		ret.setDisks(disks);
		NetworkAdapters nics = in.readObject();
		ret.setNics(nics);
		ret.setName(in.readUTF());
		ret.setResourceid(in.readUTF());
		ret.setTemplateid(in.readUTF());
		ret.setOs(in.readUTF());
		ret.setVmtype(in.readUTF());
		ret.setHypervisortype(in.readUTF());
		ret.setStatus(in.readUTF());
		ret.setTimeZone(in.readUTF());
		ret.setConfigid(in.readUTF());
		return ret;
	}

}
