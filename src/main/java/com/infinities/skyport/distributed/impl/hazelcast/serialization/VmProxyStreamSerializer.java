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
import com.infinities.skyport.compute.IVm;
import com.infinities.skyport.distributed.impl.hazelcast.HazelcastHelper;
import com.infinities.skyport.distributed.impl.hazelcast.HazelcastObjectFactory;
import com.infinities.skyport.proxy.VmProxy;

public class VmProxyStreamSerializer implements StreamSerializer<VmProxy> {

	@Override
	public int getTypeId() {
		return SerializationConstants.VM_PROXY_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, VmProxy object) throws IOException {
		out.writeObject(object.getVm());
		out.writeUTF(object.getDistributedKey());
//		out.writeBoolean(object.isLocked());
	}

	@Override
	public VmProxy read(ObjectDataInput in) throws IOException {
		IVm vm = in.readObject();
		String distributedKey = in.readUTF();
		HazelcastObjectFactory objectFactory = HazelcastHelper.getObjectFactoryByName(distributedKey);
		VmProxy ret = new VmProxy(vm, objectFactory.getAtomicLong("vm_" + vm.getVmid()), distributedKey);
//		boolean lock = in.readBoolean();
//		if (lock) {
//			ret.lock();
//		}
		return ret;
	}

}
