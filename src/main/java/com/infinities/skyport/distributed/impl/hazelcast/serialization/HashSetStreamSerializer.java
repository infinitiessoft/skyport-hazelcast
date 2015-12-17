package com.infinities.skyport.distributed.impl.hazelcast.serialization;

import java.io.IOException;
import java.util.HashSet;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

@SuppressWarnings("rawtypes")
public class HashSetStreamSerializer implements StreamSerializer<HashSet> {

	@Override
	public int getTypeId() {
		return SerializationConstants.HASHSET_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, HashSet object) throws IOException {
		out.writeInt(object.size());
		for (Object o : object) {
			out.writeObject(o);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public HashSet read(ObjectDataInput in) throws IOException {
		HashSet set = new HashSet();
		int size = in.readInt();
		for (int k = 0; k < size; k++) {
			set.add(in.readObject());
		}
		return set;
	}

}
