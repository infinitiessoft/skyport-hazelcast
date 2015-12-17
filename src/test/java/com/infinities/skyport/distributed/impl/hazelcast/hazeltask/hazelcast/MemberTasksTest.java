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
package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.hazelcast;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.hazelcast.MemberTasks.MemberResponse;

public class MemberTasksTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private IExecutorService service;
	private UUID uuid1;
	private Set<UUID> set;
	private Member member;
	private Callable<String> c;


	@Before
	public void setUp() throws Exception {
		uuid1 = UUID.randomUUID();
		service = context.mock(IExecutorService.class);
		set = new HashSet<UUID>();
		set.add(uuid1);
		member = context.mock(Member.class);
		c = new Callable<String>() {

			@Override
			public String call() throws Exception {
				return "good";
			}
		};
	}

	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteOptimisticIExecutorServiceSetOfMemberCallableOfT() throws InterruptedException,
			ExecutionException, TimeoutException {
		final Future<String> f = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(service).submitToMember(c, member);
				will(returnValue(f));

				allowing(f).get(60, TimeUnit.SECONDS);
				will(returnValue("good"));
			}
		});
		MemberResponse<String> ret = MemberTasks.executeOptimistic(service, member, c);
		assertEquals("good", ret.getValue());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteOptimisticIExecutorServiceSetOfMemberCallableOfTLongTimeUnit() throws InterruptedException,
			ExecutionException, TimeoutException {
		final Future<String> f = context.mock(Future.class);
		final Set<Member> members = new HashSet<Member>();
		members.add(member);
		final Map<Member, Future<String>> map = new HashMap<Member, Future<String>>();
		map.put(member, f);
		context.checking(new Expectations() {

			{

				allowing(service).submitToMembers(c, members);
				will(returnValue(map));

				allowing(f).get(60L, TimeUnit.SECONDS);
				will(returnValue("good"));
			}
		});
		Collection<MemberResponse<String>> ret = MemberTasks.executeOptimistic(service, members, c);
		assertEquals(1, ret.size());
		for (MemberResponse<String> r : ret) {
			assertEquals("good", r.getValue());
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteOptimisticIExecutorServiceMemberCallableOfT() throws InterruptedException, ExecutionException,
			TimeoutException {
		final Future<String> f = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(service).submitToMember(c, member);
				will(returnValue(f));

				allowing(f).get(5L, TimeUnit.SECONDS);
				will(returnValue("good"));
			}
		});
		MemberResponse<String> ret = MemberTasks.executeOptimistic(service, member, c, 5, TimeUnit.SECONDS);
		assertEquals("good", ret.getValue());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testExecuteOptimisticIExecutorServiceMemberCallableOfTLongTimeUnit() throws InterruptedException,
			ExecutionException, TimeoutException {
		final Future<String> f = context.mock(Future.class);
		final Set<Member> members = new HashSet<Member>();
		members.add(member);
		final Map<Member, Future<String>> map = new HashMap<Member, Future<String>>();
		map.put(member, f);
		context.checking(new Expectations() {

			{

				allowing(service).submitToMembers(c, members);
				will(returnValue(map));

				allowing(f).get(5L, TimeUnit.SECONDS);
				will(returnValue("good"));
			}
		});
		Collection<MemberResponse<String>> ret = MemberTasks.executeOptimistic(service, members, c, 5, TimeUnit.SECONDS);
		assertEquals(1, ret.size());
		for (MemberResponse<String> r : ret) {
			assertEquals("good", r.getValue());
		}
	}

}
