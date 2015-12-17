package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.croe.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.NamedThreadFactory;

public class NamedThreadFactoryTest {

	private NamedThreadFactory factory;


	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNewThread() {
		factory = new NamedThreadFactory("group", "name");
		Thread r = factory.newThread(new Runnable() {

			@Override
			public void run() {

			}
		});
		assertTrue(r.getName().startsWith("name-"));
		assertEquals("group", r.getThreadGroup().getName());
	}

	@Test
	public void testNewThread2() {
		factory = new NamedThreadFactory("group", "name").named("child");
		Thread r = factory.newThread(new Runnable() {

			@Override
			public void run() {

			}
		});
		assertTrue(r.getName().startsWith("name-child-"));
		assertEquals("group", r.getThreadGroup().getName());
	}
}
