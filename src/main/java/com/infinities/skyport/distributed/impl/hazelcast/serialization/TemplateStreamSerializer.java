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
import com.infinities.skyport.compute.entity.Disks;
import com.infinities.skyport.compute.entity.NetworkAdapters;
import com.infinities.skyport.compute.entity.Template;

public class TemplateStreamSerializer implements StreamSerializer<Template> {

	@Override
	public int getTypeId() {
		return SerializationConstants.TEMPLATE_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, Template object) throws IOException {
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
	public Template read(ObjectDataInput in) throws IOException {
		Template ret = new Template();
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
