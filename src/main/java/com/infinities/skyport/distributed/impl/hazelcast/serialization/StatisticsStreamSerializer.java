package com.infinities.skyport.distributed.impl.hazelcast.serialization;

import java.io.IOException;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.infinities.skyport.compute.entity.Statistics;

public class StatisticsStreamSerializer implements StreamSerializer<Statistics> {

	@Override
	public int getTypeId() {
		return SerializationConstants.STATISTICS_TYPE;
	}

	@Override
	public void destroy() {

	}

	@Override
	public void write(ObjectDataOutput out, Statistics object) throws IOException {
		out.writeObject(object.getMemusage());
		out.writeObject(object.getCpuuser());
		out.writeObject(object.getCpusys());
	}

	@Override
	public Statistics read(ObjectDataInput in) throws IOException {
		Statistics ret = new Statistics();
		Double memusage = in.readObject();
		ret.setMemusage(memusage);
		Double cpuuser = in.readObject();
		ret.setCpuuser(cpuuser);
		Double cpusys = in.readObject();
		ret.setCpusys(cpusys);
		return ret;
	}

}
