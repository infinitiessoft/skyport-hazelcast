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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;

public class MemberTasks implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(MemberTasks.class);


	public static class MemberResponse<T> implements Serializable {

		private static final long serialVersionUID = 1L;

		private T value;
		private Member member;


		public MemberResponse() {
		}

		public MemberResponse(Member member, T value) {
			this.value = value;
			this.member = member;
		}

		public T getValue() {
			return value;
		}

		public Member getMember() {
			return member;
		}
	}


	/**
	 * Will wait a maximum of 1 minute for each node to response with their
	 * result. If an error occurs on any member, we will always attempt to
	 * continue execution and collect as many results as possible.
	 * 
	 * @param execSvc
	 * @param members
	 * @param callable
	 * @return
	 */
	public static <T> Collection<MemberResponse<T>> executeOptimistic(IExecutorService execSvc, Set<Member> members,
			Callable<T> callable) {
		return executeOptimistic(execSvc, members, callable, 60, TimeUnit.SECONDS);
	}

	/**
	 * We will always try to gather as many results as possible and never throw
	 * an exception.
	 * 
	 * TODO: Make MemberResponse hold an exception that we can populate if
	 * something bad happens so we always get to return something for a member
	 * in order to indicate a failure. Getting the result when there is an error
	 * should throw an exception.
	 * 
	 * @param execSvc
	 * @param members
	 * @param callable
	 * @param maxWaitTime
	 *            - a value of 0 indicates forever
	 * @param unit
	 * @return
	 */
	public static <T> Collection<MemberResponse<T>> executeOptimistic(IExecutorService execSvc, Set<Member> members,
			Callable<T> callable, long maxWaitTime, TimeUnit unit) {
		Collection<MemberResponse<T>> result = new ArrayList<MemberResponse<T>>(members.size());

		Map<Member, Future<T>> resultFutures = execSvc.submitToMembers(callable, members);
		for (Entry<Member, Future<T>> futureEntry : resultFutures.entrySet()) {
			Future<T> future = futureEntry.getValue();
			Member member = futureEntry.getKey();

			try {
				if (maxWaitTime > 0) {
					result.add(new MemberResponse<T>(member, future.get(maxWaitTime, unit)));
				} else {
					result.add(new MemberResponse<T>(member, future.get()));
				}
				// ignore exceptions... return what you can
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt(); // restore interrupted
													// status and return what we
													// have
				return result;
			} catch (MemberLeftException e) {
				logger.warn("Member {} left while trying to get a distributed callable result", member);
			} catch (ExecutionException e) {
				if (e.getCause() instanceof InterruptedException) {
					// restore interrupted state and return
					Thread.currentThread().interrupt();
					return result;
				} else {
					logger.warn("Unable to execute callable on " + member + ". There was an error.", e);
				}
			} catch (TimeoutException e) {
				logger.error("Unable to execute task on " + member + " within 10 seconds.");
			} catch (RuntimeException e) {
				logger.error("Unable to execute task on " + member + ". An unexpected error occurred.", e);
			}
		}

		return result;
	}

	public static <T> MemberResponse<T> executeOptimistic(IExecutorService execSvc, Member member, Callable<T> callable) {
		return executeOptimistic(execSvc, member, callable, 60, TimeUnit.SECONDS);
	}

	public static <T> MemberResponse<T> executeOptimistic(IExecutorService execSvc, Member member, Callable<T> callable,
			long maxWaitTime, TimeUnit unit) {
		Future<T> future = execSvc.submitToMember(callable, member);
		try {
			if (maxWaitTime > 0) {
				return new MemberResponse<T>(member, future.get(maxWaitTime, unit));
			} else {
				return new MemberResponse<T>(member, future.get());
			}
			// ignore exceptions... return what you can
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt(); // restore interrupted
												// status and return what we
												// have
		} catch (MemberLeftException e) {
			logger.warn("Member {} left while trying to get a distributed callable result", member);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof InterruptedException) {
				// restore interrupted state and return
				Thread.currentThread().interrupt();
			} else {
				logger.warn("Unable to execute callable on " + member + ". There was an error.", e);
			}
		} catch (TimeoutException e) {
			logger.error("Unable to execute task on " + member + " within 10 seconds.");
		} catch (RuntimeException e) {
			logger.error("Unable to execute task on " + member + ". An unexpected error occurred.", e);
		}
		// }

		return null;
	}


	public static class MemberResponseCallable<T> implements Callable<MemberResponse<T>>, Serializable {

		private static final long serialVersionUID = 1L;
		private Callable<T> delegate;
		private Member member;


		public MemberResponseCallable(Callable<T> delegate, Member member) {
			this.delegate = delegate;
			this.member = member;
		}

		public Member getMember() {
			return this.member;
		}

		public Callable<T> getDelegate() {
			return delegate;
		}

		@Override
		public MemberResponse<T> call() throws Exception {
			return new MemberResponse<T>(member, delegate.call());
		}
	}
}
