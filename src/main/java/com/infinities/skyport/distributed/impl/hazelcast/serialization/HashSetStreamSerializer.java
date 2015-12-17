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
