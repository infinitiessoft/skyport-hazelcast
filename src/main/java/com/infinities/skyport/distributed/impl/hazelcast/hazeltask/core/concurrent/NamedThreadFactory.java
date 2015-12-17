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
package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent;

import java.io.Serializable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final transient ThreadGroup group;
	private final AtomicInteger threadNumber = new AtomicInteger(0);
	private final String namePrefix;


	protected NamedThreadFactory(String namePrefix, ThreadGroup group) {
		this.namePrefix = namePrefix;
		this.group = group;
	}

	public NamedThreadFactory(String groupName, String threadNamePrefix) {
		SecurityManager s = System.getSecurityManager();
		ThreadGroup parent = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();

		group = new ThreadGroup(parent, groupName);
		namePrefix = threadNamePrefix;
	}

	public ThreadGroup getThreadGroup() {
		return group;
	}

	public String getNamePrefix() {
		return this.namePrefix;
	}

	protected boolean getDaemon() {
		return false;
	}

	protected int getPriority() {
		return Thread.NORM_PRIORITY;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(group, r, namePrefix + "-" + threadNumber.getAndIncrement(), 0);
		t.setDaemon(getDaemon());
		t.setPriority(getPriority());
		return t;
	}


	protected static class ChildNamedThreadFactory extends NamedThreadFactory {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final NamedThreadFactory parent;


		protected ChildNamedThreadFactory(NamedThreadFactory parent, String childName) {
			super(parent.getNamePrefix() + "-" + childName, parent.getThreadGroup());
			this.parent = parent;
		}

		@Override
		protected boolean getDaemon() {
			return parent.getDaemon();
		}

		@Override
		protected int getPriority() {
			return parent.getPriority();
		}

	}


	/**
	 * Creates a NamedThreadFactory that is a "child" of this instance. The name
	 * provided here will be appended to this instance's name in threads created
	 * from the returned NamedThreadFactory
	 * 
	 * @param name
	 * @return
	 */
	public NamedThreadFactory named(String name) {
		return new ChildNamedThreadFactory(this, name);
	}

}
