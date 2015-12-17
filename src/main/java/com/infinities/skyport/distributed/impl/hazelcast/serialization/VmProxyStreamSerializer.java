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
