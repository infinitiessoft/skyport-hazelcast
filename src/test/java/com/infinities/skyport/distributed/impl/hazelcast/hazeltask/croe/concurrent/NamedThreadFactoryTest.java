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
