package com.infinities.skyport.distributed.impl.hazelcast.serialization;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.infinities.skyport.compute.entity.Storage;

public class StorageStreamSerializer implements StreamSerializer<Storage> {

	@Override
	public int getTypeId() {
		return SerializationConstants.STORAGE_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, Storage object) throws IOException {
		out.writeUTF(object.getName());
		out.writeUTF(object.getStorageDomainId());
		out.writeUTF(object.getDomainType());
		out.writeUTF(object.getType());
		out.writeObject(object.getAvailableDiskSize());
		out.writeObject(object.getUsedDiskSize());
		out.writeUTF(object.getConfig());
		out.writeUTF(object.getConfigid());
	}

	@Override
	public Storage read(ObjectDataInput in) throws IOException {
		Storage ret = new Storage();
		// AbstractEntity
		ret.setName(in.readUTF());
		ret.setStorageDomainId(in.readUTF());
		ret.setDomainType(in.readUTF());
		ret.setType(in.readUTF());
		Long availableDiskSize = in.readObject();
		ret.setAvailableDiskSize(availableDiskSize);
		Long usedDiskSize = in.readObject();
		ret.setUsedDiskSize(usedDiskSize);
		ret.setConfig(in.readUTF());
		ret.setConfigid(in.readUTF());
		return ret;
	}

}
