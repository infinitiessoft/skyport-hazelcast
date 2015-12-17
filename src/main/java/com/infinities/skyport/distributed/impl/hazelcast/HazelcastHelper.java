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
package com.infinities.skyport.distributed.impl.hazelcast;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.infinities.skyport.distributed.DistributedObjectFactory;
import com.infinities.skyport.util.PropertiesHolder;

public class HazelcastHelper implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(HazelcastHelper.class);
	public static String HAZELCAST_FILE = PropertiesHolder.CONFIG_FOLDER + File.separator + "hazelcast.xml";
	protected static final ConcurrentHashMap<String, HazelcastObjectFactory> instances = new ConcurrentHashMap<String, HazelcastObjectFactory>();


	private HazelcastHelper() {

	}

	public synchronized static HazelcastObjectFactory getObjectFactoryByName(String topology) throws FileNotFoundException {
		HazelcastObjectFactory instance = instances.get(topology);
		if (instance == null) {
			instance = newHazelcastObjectFactory(topology);
		}

		return instance;
	}

	private synchronized static HazelcastObjectFactory newHazelcastObjectFactory(String topologyName)
			throws FileNotFoundException {
		HazelcastObjectFactory factory = new HazelcastObjectFactory(topologyName);
		if (instances.putIfAbsent(topologyName, factory) != null) {
			throw new IllegalStateException("An instance for the name " + topologyName + " already exists!");
		}

		return factory;
	}

	public synchronized static HazelcastInstance getHazelcastInstance(String groupName) {
		HazelcastInstance hz = Hazelcast.getHazelcastInstanceByName(groupName);
		if (hz == null) {
			Config config;
			String configFileLocation = HAZELCAST_FILE;
			try {
				logger.debug("Load HAZELCAST File: {}", configFileLocation);
				config = new XmlConfigBuilder(configFileLocation).build();
			} catch (FileNotFoundException e) {
				logger.warn("File " + configFileLocation + " not found", e);
				config = new Config();
			}
			config.setInstanceName(groupName);
			config.getGroupConfig().setName(config.getGroupConfig().getName() + "_" + groupName);
			hz = Hazelcast.newHazelcastInstance(config);
			logger.debug("HazelcastInstance: {} is created", new Object[] { hz.toString() });
		}
		return hz;
	}

	public synchronized static HazelcastInstance getDefaultInstance() {
		return getHazelcastInstance(DistributedObjectFactory.DEFAULT_UNIT_NAME);
	}

	public static String getGroup() throws FileNotFoundException {
		Config config;
		String configFileLocation = HAZELCAST_FILE;
		logger.debug("Load HAZELCAST File: {}", configFileLocation);
		config = new XmlConfigBuilder(configFileLocation).build();
		return config.getGroupConfig().getName();
	}
}
