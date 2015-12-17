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
