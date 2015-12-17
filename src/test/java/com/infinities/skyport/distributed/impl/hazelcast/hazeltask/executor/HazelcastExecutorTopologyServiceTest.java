package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
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
import org.jmock.api.Invocation;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.action.CustomAction;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.hazelcast.query.SqlPredicate;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop.CancelReturnOp;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop.CancelTaskOp;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop.CompletedTaskOp;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.clusterop.FailedTaskOp;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTask;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.HazeltaskTaskImpl;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.TaskResponse;

public class HazelcastExecutorTopologyServiceTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private HazelcastExecutorTopologyService service;
	private String name = "test";
	private HazelcastInstance hazelcast;
	private Cluster cluster;
	private Member member;
	private IExecutorService executor;
	@SuppressWarnings("rawtypes")
	private IMap map;
	@SuppressWarnings("rawtypes")
	private ITopic topic;
	private String uuid1;
	private UUID memberid;
	private UUID memberid2;
	private Set<Member> members;
	private Member member2;


	@Before
	public void setUp() throws Exception {
		uuid1 = UUID.randomUUID().toString();
		memberid = UUID.randomUUID();
		memberid2 = UUID.randomUUID();
		hazelcast = context.mock(HazelcastInstance.class);
		cluster = context.mock(Cluster.class);
		member = context.mock(Member.class);
		executor = context.mock(IExecutorService.class);
		map = context.mock(IMap.class);
		topic = context.mock(ITopic.class);
		member2 = context.mock(Member.class, "member2");
		context.checking(new Expectations() {

			{
				oneOf(hazelcast).getCluster();
				will(returnValue(cluster));

				oneOf(cluster).getLocalMember();
				will(returnValue(member));

				oneOf(hazelcast).getExecutorService("test-com");
				will(returnValue(executor));

				oneOf(hazelcast).getMap("test-pending-tasks");
				will(returnValue(map));

				oneOf(hazelcast).getTopic("test-task-response");
				will(returnValue(topic));
			}
		});
		service = new HazelcastExecutorTopologyService(name, hazelcast);
		members = new HashSet<Member>();
		members.add(member);
		members.add(member2);
	}

	@After
	public void tearDown() throws Exception {
		context.assertIsSatisfied();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddPendingTask() {
		final HazeltaskTask<String> task = newTask();
		context.checking(new Expectations() {

			{

				oneOf(map).putIfAbsent(uuid1, task);
				will(returnValue(null));
			}
		});

		assertTrue(service.addPendingTask(task, false));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddPendingTask2() {
		final HazeltaskTask<String> task = newTask();
		context.checking(new Expectations() {

			{

				oneOf(map).putIfAbsent(uuid1, task);
				will(returnValue(newTask()));
			}
		});

		assertFalse(service.addPendingTask(task, false));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddPendingTask3() {
		final HazeltaskTask<String> task = newTask();
		context.checking(new Expectations() {

			{

				oneOf(map).put(uuid1, task);
				will(returnValue(newTask()));
			}
		});

		assertTrue(service.addPendingTask(task, true));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddPendingTask4() {
		final HazeltaskTask<String> task = newTask();
		context.checking(new Expectations() {

			{

				oneOf(map).put(uuid1, task);
				will(returnValue(null));
			}
		});
		assertTrue(service.addPendingTask(task, true));
	}

	@Test
	public void testRemovePendingTaskT() {
		final HazeltaskTask<String> task = newTask();
		context.checking(new Expectations() {

			{

				oneOf(map).remove(uuid1);
				will(returnValue(task));
			}
		});
		assertTrue(service.removePendingTask(task));
	}

	@Test
	public void testRemovePendingTaskT2() {
		final HazeltaskTask<String> task = newTask();
		context.checking(new Expectations() {

			{

				oneOf(map).remove(uuid1);
				will(returnValue(null));
			}
		});
		assertFalse(service.removePendingTask(task));
	}

	@Test
	public void testRemovePendingTaskT3() {
		final HazeltaskTask<String> task = newTask();
		context.checking(new Expectations() {

			{

				oneOf(map).remove(uuid1);
				will(returnValue(task));
			}
		});
		assertTrue(service.removePendingTask(uuid1));
	}

	@Test
	public void testRemovePendingTaskT4() {
		context.checking(new Expectations() {

			{

				oneOf(map).remove(uuid1);
				will(returnValue(null));
			}
		});
		assertFalse(service.removePendingTask(uuid1));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskCompletionUUIDSerializable() throws InterruptedException, ExecutionException,
			TimeoutException {
		final Future<Serializable> future = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(member).getUuid();
				will(returnValue(memberid.toString()));

				allowing(member2).getUuid();
				will(returnValue(memberid2.toString()));

				exactly(1).of(executor).submitToMember(with(any(CompletedTaskOp.class)), with(any(Member.class)));
				will(returnValue(future));

				exactly(1).of(future).get(60, TimeUnit.SECONDS);
				will(returnValue(true));

				oneOf(cluster).getMembers();
				will(returnValue(members));
			}
		});
		assertTrue(service.broadcastTaskCompletion(uuid1, memberid2.toString(), "test"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskCompletionUUIDSerializable1() throws InterruptedException, ExecutionException,
			TimeoutException {
		final Future<Serializable> future = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(member).getUuid();
				will(returnValue(memberid.toString()));

				allowing(member2).getUuid();
				will(returnValue(memberid2.toString()));

				exactly(1).of(executor).submitToMember(with(any(CompletedTaskOp.class)), with(any(Member.class)));
				will(returnValue(future));

				exactly(1).of(future).get(60, TimeUnit.SECONDS);
				will(returnValue(false));

				oneOf(cluster).getMembers();
				will(returnValue(members));
			}
		});
		assertFalse(service.broadcastTaskCompletion(uuid1, memberid2.toString(), "test"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskCompletionUUIDSerializable2() throws InterruptedException, ExecutionException,
			TimeoutException {
		final Future<Serializable> future = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(member).getUuid();
				will(returnValue(memberid.toString()));

				allowing(member2).getUuid();
				will(returnValue(memberid2.toString()));

				exactly(1).of(executor).submitToMember(with(any(CompletedTaskOp.class)), with(any(Member.class)));
				will(returnValue(future));

				exactly(1).of(future).get(60, TimeUnit.SECONDS);
				will(new CustomAction("timeout") {

					@Override
					public Object invoke(Invocation invocation) throws Throwable {
						throw new TimeoutException("timeout");
					}
				});

				oneOf(cluster).getMembers();
				will(returnValue(members));
			}
		});
		assertFalse(service.broadcastTaskCompletion(uuid1, memberid2.toString(), "test"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskCancellationUUID() throws InterruptedException, ExecutionException, TimeoutException {
		final Future<Serializable> future = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(member).getUuid();
				will(returnValue(memberid.toString()));

				allowing(member2).getUuid();
				will(returnValue(memberid2.toString()));

				exactly(1).of(executor).submitToMember(with(any(CancelReturnOp.class)), with(any(Member.class)));
				will(returnValue(future));

				exactly(1).of(future).get(60, TimeUnit.SECONDS);
				will(returnValue(true));

				oneOf(cluster).getMembers();
				will(returnValue(members));
			}
		});
		assertTrue(service.broadcastTaskCancellation(uuid1, memberid2.toString()));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskCancellationUUID2() throws InterruptedException, ExecutionException, TimeoutException {
		final Future<Serializable> future = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(member).getUuid();
				will(returnValue(memberid.toString()));

				allowing(member2).getUuid();
				will(returnValue(memberid2.toString()));

				exactly(1).of(executor).submitToMember(with(any(CancelReturnOp.class)), with(any(Member.class)));
				will(returnValue(future));

				exactly(1).of(future).get(60, TimeUnit.SECONDS);
				will(returnValue(false));

				oneOf(cluster).getMembers();
				will(returnValue(members));
			}
		});
		assertFalse(service.broadcastTaskCancellation(uuid1, memberid2.toString()));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskCancellationUUID3() throws InterruptedException, ExecutionException, TimeoutException {
		final Future<Serializable> future = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(member).getUuid();
				will(returnValue(memberid.toString()));

				allowing(member2).getUuid();
				will(returnValue(memberid2.toString()));

				exactly(1).of(executor).submitToMember(with(any(CancelReturnOp.class)), with(any(Member.class)));
				will(returnValue(future));

				exactly(1).of(future).get(60, TimeUnit.SECONDS);
				will(new CustomAction("timeout") {

					@Override
					public Object invoke(Invocation invocation) throws Throwable {
						throw new TimeoutException("timeout");
					}
				});

				oneOf(cluster).getMembers();
				will(returnValue(members));
			}
		});
		assertFalse(service.broadcastTaskCancellation(uuid1, memberid2.toString()));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskErrorUUIDThrowable() throws InterruptedException, ExecutionException, TimeoutException {
		Exception t = new IllegalStateException("test");
		final Future<Serializable> future = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(member).getUuid();
				will(returnValue(memberid.toString()));

				allowing(member2).getUuid();
				will(returnValue(memberid2.toString()));

				exactly(1).of(executor).submitToMember(with(any(FailedTaskOp.class)), with(any(Member.class)));
				will(returnValue(future));

				exactly(1).of(future).get(60, TimeUnit.SECONDS);
				will(returnValue(true));

				oneOf(cluster).getMembers();
				will(returnValue(members));
			}
		});
		assertTrue(service.broadcastTaskError(uuid1, memberid2.toString(), t));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskErrorUUIDThrowable2() throws InterruptedException, ExecutionException, TimeoutException {
		Exception t = new IllegalStateException("test");
		final Future<Serializable> future = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(member).getUuid();
				will(returnValue(memberid.toString()));

				allowing(member2).getUuid();
				will(returnValue(memberid2.toString()));

				exactly(1).of(executor).submitToMember(with(any(FailedTaskOp.class)), with(any(Member.class)));
				will(returnValue(future));

				exactly(1).of(future).get(60, TimeUnit.SECONDS);
				will(returnValue(false));

				oneOf(cluster).getMembers();
				will(returnValue(members));
			}
		});
		assertFalse(service.broadcastTaskError(uuid1, memberid2.toString(), t));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskErrorUUIDThrowable3() throws InterruptedException, ExecutionException, TimeoutException {
		Exception t = new IllegalStateException("test");
		final Future<Serializable> future = context.mock(Future.class);
		context.checking(new Expectations() {

			{

				allowing(member).getUuid();
				will(returnValue(memberid.toString()));

				allowing(member2).getUuid();
				will(returnValue(memberid2.toString()));

				exactly(1).of(executor).submitToMember(with(any(FailedTaskOp.class)), with(any(Member.class)));
				will(returnValue(future));

				exactly(1).of(future).get(60, TimeUnit.SECONDS);
				will(new CustomAction("timeout") {

					@Override
					public Object invoke(Invocation invocation) throws Throwable {
						throw new TimeoutException("timeout");
					}
				});

				oneOf(cluster).getMembers();
				will(returnValue(members));
			}
		});
		assertFalse(service.broadcastTaskError(uuid1, memberid2.toString(), t));
	}

	@Test
	public void testGetLocalPendingTaskKeysString() {
		final Set<String> keys = new HashSet<String>();
		keys.add(uuid1);
		final Map<String, HazeltaskTask<String>> taskMap = new HashMap<String, HazeltaskTask<String>>();
		taskMap.put(uuid1, newTask());

		context.checking(new Expectations() {

			{

				oneOf(map).localKeySet(with(any(SqlPredicate.class)));
				will(returnValue(keys));
			}
		});
		assertEquals(keys, service.getLocalPendingTaskKeys("test"));
	}

	@Test
	public void testGetLocalPendingTaskKeys() {
		final Set<String> keys = new HashSet<String>();
		keys.add(uuid1);
		final Map<String, HazeltaskTask<String>> taskMap = new HashMap<String, HazeltaskTask<String>>();
		taskMap.put(uuid1, newTask());

		context.checking(new Expectations() {

			{

				oneOf(map).localKeySet();
				will(returnValue(keys));
			}
		});
		assertEquals(keys, service.getLocalPendingTaskKeys());
	}

	@Test
	public void testGetLocalPendingTaskMapSize() {
		final Set<String> keys = new HashSet<String>();
		keys.add(uuid1);
		final Map<String, HazeltaskTask<String>> taskMap = new HashMap<String, HazeltaskTask<String>>();
		taskMap.put(uuid1, newTask());

		context.checking(new Expectations() {

			{

				oneOf(map).localKeySet();
				will(returnValue(keys));
			}
		});
		assertEquals(1, service.getLocalPendingTaskMapSize());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddTaskResponseMessageHandler() {
		final MessageListener<TaskResponse<Object>> listener = context.mock(MessageListener.class);
		context.checking(new Expectations() {

			{

				oneOf(topic).addMessageListener(listener);
				will(returnValue("test"));
			}
		});
		service.addTaskResponseMessageHandler(listener);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCancelTask() throws InterruptedException, ExecutionException, TimeoutException {
		final Future<Serializable> future = context.mock(Future.class);
		final Map<Member, Future<Serializable>> futureMap = new HashMap<Member, Future<Serializable>>();
		futureMap.put(member, future);
		context.checking(new Expectations() {

			{

				allowing(member).getUuid();
				will(returnValue(memberid.toString()));

				allowing(member2).getUuid();
				will(returnValue(memberid2.toString()));

				exactly(1).of(executor).submitToMembers(with(any(CancelTaskOp.class)), with(any(Set.class)));
				will(returnValue(futureMap));

				exactly(1).of(future).get(60, TimeUnit.SECONDS);
				will(returnValue(true));

				oneOf(cluster).getMembers();
				will(returnValue(members));
			}
		});
		assertTrue(service.cancelTask(uuid1));
	}

	@Test
	public void testGetPendingTasks() {
		assertEquals(map, service.getPendingTasks());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testAddLocalEntryListener() {
		final EntryListener<String, HazeltaskTask<?>> listener = context.mock(EntryListener.class);
		context.checking(new Expectations() {

			{

				oneOf(map).addLocalEntryListener(listener);
				will(returnValue("test"));
			}
		});

		service.addLocalEntryListener(listener);
	}

	@Test
	public void testGetLocalMember() {
		assertEquals(member, service.getLocalMember());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskCompletionUUIDStringSerializable() {
		context.checking(new Expectations() {

			{

				oneOf(member).getUuid();
				will(returnValue(memberid.toString()));

				oneOf(topic).publish(with(any(TaskResponse.class)));
				will(new CustomAction("complete") {

					@Override
					public Object invoke(Invocation invocation) throws Throwable {
						TaskResponse<String> response = (TaskResponse<String>) invocation.getParameter(0);
						assertEquals(TaskResponse.Status.SUCCESS, response.getStatus());
						assertEquals("test", response.getResponse());
						assertNull(response.getError());
						return null;
					}
				});
			}
		});
		service.broadcastTaskCompletion(uuid1, "test");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskErrorUUIDStringThrowable() {
		final Throwable t = new IllegalStateException("test");
		context.checking(new Expectations() {

			{

				oneOf(member).getUuid();
				will(returnValue(memberid.toString()));

				oneOf(topic).publish(with(any(TaskResponse.class)));
				will(new CustomAction("complete") {

					@Override
					public Object invoke(Invocation invocation) throws Throwable {
						TaskResponse<String> response = (TaskResponse<String>) invocation.getParameter(0);
						assertEquals(TaskResponse.Status.FAILURE, response.getStatus());
						assertEquals(t, response.getError());
						assertNull(response.getResponse());
						return null;
					}
				});
			}
		});
		service.broadcastTaskError(uuid1, t);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBroadcastTaskCancellationUUIDString() {
		context.checking(new Expectations() {

			{

				oneOf(member).getUuid();
				will(returnValue(memberid.toString()));

				oneOf(topic).publish(with(any(TaskResponse.class)));
				will(new CustomAction("complete") {

					@Override
					public Object invoke(Invocation invocation) throws Throwable {
						TaskResponse<String> response = (TaskResponse<String>) invocation.getParameter(0);
						assertEquals(TaskResponse.Status.CANCELLED, response.getStatus());
						assertNull(response.getError());
						assertNull(response.getResponse());
						return null;
					}
				});
			}
		});
		service.broadcastTaskCancellation(uuid1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetLocalPendingTasksString() {
		final Set<String> keys = new HashSet<String>();
		keys.add(uuid1);
		final Map<String, HazeltaskTask<String>> taskMap = new HashMap<String, HazeltaskTask<String>>();
		taskMap.put(uuid1, newTask());

		context.checking(new Expectations() {

			{

				oneOf(map).localKeySet(with(any(SqlPredicate.class)));
				will(returnValue(keys));
				oneOf(map).getAll(keys);
				will(returnValue(taskMap));
			}
		});
		assertEquals(taskMap.values(), service.getLocalPendingTasks("test"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetLocalPendingTasks() {
		final Set<String> keys = new HashSet<String>();
		keys.add(uuid1);
		final Map<String, HazeltaskTask<String>> taskMap = new HashMap<String, HazeltaskTask<String>>();
		taskMap.put(uuid1, newTask());

		context.checking(new Expectations() {

			{

				oneOf(map).localKeySet();
				will(returnValue(keys));
				oneOf(map).getAll(keys);
				will(returnValue(taskMap));
			}
		});
		assertEquals(taskMap.values(), service.getLocalPendingTasks());
	}

	private HazeltaskTask<String> newTask() {
		HazeltaskTask<String> task = new HazeltaskTaskImpl<String>(uuid1, memberid.toString(), new Callable<String>() {

			@Override
			public String call() throws Exception {
				return "good";
			}
		});
		return task;
	}

}
