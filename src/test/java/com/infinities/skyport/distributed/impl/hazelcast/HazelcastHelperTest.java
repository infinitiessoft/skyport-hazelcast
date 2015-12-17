package com.infinities.skyport.distributed.impl.hazelcast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.io.FileNotFoundException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.HazelcastInstance;

public class HazelcastHelperTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetObjectFactoryByName() throws FileNotFoundException {
		assertEquals(0, HazelcastHelper.instances.size());
		HazelcastObjectFactory factory = HazelcastHelper.getObjectFactoryByName("test");
		assertNotNull(factory);
		assertEquals(1, HazelcastHelper.instances.size());
		HazelcastObjectFactory factory2 = HazelcastHelper.getObjectFactoryByName("test");
		assertEquals(factory, factory2);
		assertEquals(1, HazelcastHelper.instances.size());
		HazelcastObjectFactory factory3 = HazelcastHelper.getObjectFactoryByName("test2");
		assertNotEquals(factory, factory3);
		assertEquals(2, HazelcastHelper.instances.size());
	}

	@Test
	public void testGetHazelcastInstance() {
		HazelcastInstance instance = HazelcastHelper.getHazelcastInstance("test");
		assertNotNull(instance);
		HazelcastInstance instance2 = HazelcastHelper.getHazelcastInstance("test");
		assertEquals(instance, instance2);
		HazelcastInstance instance3 = HazelcastHelper.getHazelcastInstance("test2");
		assertNotEquals(instance, instance3);
	}

	@Test
	public void testGetDefaultInstance() {
		HazelcastInstance instance = HazelcastHelper.getDefaultInstance();
		assertNotNull(instance);
		HazelcastInstance instance2 = HazelcastHelper.getDefaultInstance();
		assertEquals(instance, instance2);
	}
}
