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
import java.util.Date;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.infinities.skyport.compute.entity.ComputeObject;
import com.infinities.skyport.compute.entity.Disk;

public class DiskStreamSerializer implements StreamSerializer<Disk> {

	@Override
	public int getTypeId() {
		return SerializationConstants.DISK_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, Disk object) throws IOException {
		// AbstractEntity
		out.writeObject(object.getId());
		out.writeUTF(object.getDesc());
		out.writeInt(object.getVersion());
		// AbstractVirtEntity
		out.writeUTF(object.getConfig());
		// Disk
		out.writeUTF(object.getSnapshotid());
		out.writeObject(object.getSizegb());
		out.writeObject(object.getTruesize());
		out.writeObject(object.getApparentsize());
		out.writeObject(object.getReadrate());
		out.writeObject(object.getWriterate());
		out.writeUTF(object.getStatus());
		out.writeUTF(object.getVolumeformat());
		out.writeUTF(object.getVolumetype());
		out.writeUTF(object.getDisktype());
		out.writeObject(object.getCreationdate());
		out.writeUTF(object.getInternaldrivemapping());
		out.writeObject(object.getBoot());
		out.writeUTF(object.getDiskinterface());
		out.writeUTF(object.getWipeafterdelete());
		out.writeUTF(object.getPropagateerrors());
		out.writeUTF(object.getDiskid());
		out.writeUTF(object.getName());
		out.writeUTF(object.getInstanceid());
		out.writeUTF(object.getInstancetype());
		out.writeObject(object.getActualsizegb());
		out.writeObject(object.getTemplateBaseEntity());
		out.writeUTF(object.getVmId());
		out.writeUTF(object.getStorageId());
		out.writeUTF(object.getConfigid());
	}

	@Override
	public Disk read(ObjectDataInput in) throws IOException {
		Disk ret = new Disk();
		// AbstractEntity
		Long id = in.readObject();
		ret.setId(id);
		ret.setDesc(in.readUTF());
		ret.setVersion(in.readInt());
		// AbstractVirtEntity
		ret.setConfig(in.readUTF());
		// Disk
		ret.setSnapshotid(in.readUTF());
		Long sizegb = in.readObject();
		ret.setSizegb(sizegb);
		Long truesize = in.readObject();
		ret.setTruesize(truesize);
		Long apparentsize = in.readObject();
		ret.setApparentsize(apparentsize);
		Double readrate = in.readObject();
		ret.setReadrate(readrate);
		Double writerate = in.readObject();
		ret.setWriterate(writerate);
		ret.setStatus(in.readUTF());
		ret.setVolumeformat(in.readUTF());
		ret.setVolumetype(in.readUTF());
		ret.setDisktype(in.readUTF());
		Date creationdate = in.readObject();
		ret.setCreationdate(creationdate);
		ret.setInternaldrivemapping(in.readUTF());
		Boolean boot = in.readObject();
		ret.setBoot(boot);
		ret.setDiskinterface(in.readUTF());
		ret.setWipeafterdelete(in.readUTF());
		ret.setPropagateerrors(in.readUTF());
		ret.setDiskid(in.readUTF());
		ret.setName(in.readUTF());
		ret.setInstanceid(in.readUTF());
		ret.setInstancetype(in.readUTF());
		Long actualsizegb = in.readObject();
		ret.setActualsizegb(actualsizegb);
		ComputeObject templateBaseEntity = in.readObject();
		ret.setTemplateBaseEntity(templateBaseEntity);
		ret.setVmId(in.readUTF());
		ret.setStorageId(in.readUTF());
		ret.setConfigid(in.readUTF());
		return ret;
	}

}
