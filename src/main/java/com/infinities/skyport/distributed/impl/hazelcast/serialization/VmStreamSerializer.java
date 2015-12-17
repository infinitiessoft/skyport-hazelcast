package com.infinities.skyport.distributed.impl.hazelcast.serialization;

import java.io.IOException;
import java.util.Date;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.infinities.skyport.compute.entity.Disks;
import com.infinities.skyport.compute.entity.NetworkAdapters;
import com.infinities.skyport.compute.entity.Statistics;
import com.infinities.skyport.compute.entity.Vm;

public class VmStreamSerializer implements StreamSerializer<Vm> {

	@Override
	public int getTypeId() {
		return SerializationConstants.VM_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, Vm object) throws IOException {
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
		// Vm
		out.writeUTF(object.getVmid());
		out.writeUTF(object.getRunningonhostid());
		out.writeUTF(object.getIp());
		out.writeUTF(object.getCdisopath());
		out.writeUTF(object.getInfo());
		out.writeObject(object.getStatistics());
		out.writeUTF(object.getDomain());
		out.writeUTF(object.getDisplayType());
		out.writeUTF(object.getBootDevice());
		out.writeUTF(object.getFlavorId());
		out.writeUTF(object.getKeyName());
		out.writeUTF(object.getType());
		out.writeUTF(object.getGroup());
		out.writeUTF(object.getKey());
		out.writeUTF(object.getConfigid());
	}

	@Override
	public Vm read(ObjectDataInput in) throws IOException {
		Vm ret = new Vm();
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
		// Vm
		ret.setVmid(in.readUTF());
		ret.setRunningonhostid(in.readUTF());
		ret.setIp(in.readUTF());
		ret.setCdisopath(in.readUTF());
		ret.setInfo(in.readUTF());
		Statistics statistics = in.readObject();
		ret.setStatistics(statistics);
		ret.setDomain(in.readUTF());
		ret.setDisplayType(in.readUTF());
		ret.setBootDevice(in.readUTF());
		ret.setFlavorId(in.readUTF());
		ret.setKeyName(in.readUTF());
		ret.setType(in.readUTF());
		ret.setGroup(in.readUTF());
		ret.setKey(in.readUTF());
		ret.setConfigid(in.readUTF());
		return ret;
	}

}
