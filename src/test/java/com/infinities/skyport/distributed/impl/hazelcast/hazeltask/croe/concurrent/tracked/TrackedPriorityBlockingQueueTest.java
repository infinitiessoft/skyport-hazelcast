package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.croe.concurrent.tracked;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.core.concurrent.collections.tracked.TrackedPriorityBlockingQueue;
import com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor.task.Task;

public class TrackedPriorityBlockingQueueTest {

	private TrackedPriorityBlockingQueue<TestTask> queue;

	private TestTask task1 = new TestTask(UUID.randomUUID().toString(), 1);
	private TestTask task2 = new TestTask(UUID.randomUUID().toString(), 2);
	private TestTask task3 = new TestTask(UUID.randomUUID().toString(), 3);
	private TestTask task4 = new TestTask(UUID.randomUUID().toString(), 4);
	private TestTask task5 = new TestTask(UUID.randomUUID().toString(), 5);
	private TestTask task6 = new TestTask(UUID.randomUUID().toString(), 6);
	private TestTask task7 = new TestTask(UUID.randomUUID().toString(), 7);


	@Before
	public void setUp() throws Exception {
		queue = new TrackedPriorityBlockingQueue<TestTask>();
		queue.offer(task4);
		queue.offer(task5);
		queue.offer(task6);
		queue.offer(task1);
		queue.offer(task2);
		queue.offer(task3);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testPollOrder() throws InterruptedException {
		assertEquals(task1, queue.take());
		assertEquals(task2, queue.take());
		assertEquals(task3, queue.take());
		assertEquals(task4, queue.take());
		assertEquals(task5, queue.take());
		assertEquals(task6, queue.take());
	}

	@Test
	public void testGetOldestItemTime() {
		assertEquals(1L, (long) queue.getOldestItemTime());
	}

	@Test
	public void testGetLastAddedTime() throws InterruptedException {
		Thread.sleep(10);
		// unless a machine is really really slow... this should be ok
		long now = System.currentTimeMillis();
		queue.offer(task7);

		assertTrue(timeBuffer(now, queue.getLastAddedTime()));
	}

	@Test
	public void testGetLastRemovedTime() throws InterruptedException {
		Thread.sleep(10);
		// unless a machine is really really slow... this should be ok
		long now = System.currentTimeMillis();
		queue.poll();

		assertTrue(timeBuffer(now, queue.getLastRemovedTime()));
	}

	@Test
	public void getOldestTimeNull() {
		queue.clear();
		assertNull(queue.getOldestItemTime());
	}

	@Test
	public void getLastAddedTimeNull() {
		TrackedPriorityBlockingQueue<TestTask> queue = new TrackedPriorityBlockingQueue<TestTask>();
		queue.clear();
		assertNull(queue.getLastAddedTime());
	}

	@Test
	public void getLastRemovedTimeNull() {
		queue.clear();
		assertNull(queue.getLastRemovedTime());
	}

	@Test
	public void remove() throws InterruptedException {
		Thread.sleep(10);
		long now = System.currentTimeMillis();
		boolean removed = queue.remove(task1);
		assertTrue(removed);
		assertTrue(timeBuffer(now, queue.getLastRemovedTime()));
	}

	private boolean timeBuffer(long expected, long actual) {
		long buffer = 2;
		return actual <= (expected + buffer) && actual >= (expected - buffer);
	}


	static class TestTask implements Task<String> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String id;
		private long createdTime;


		public TestTask(String id, long createdTime) {
			this.id = id;
			this.createdTime = createdTime;
		}

		@Override
		public void run() {
			System.out.println("task: " + id + ", createedTime: " + createdTime);
		}

		@Override
		public long getTimeCreated() {
			return createdTime;
		}

		@Override
		public String getId() {
			return id;
		}

		@Override
		public String getResult() {
			return null;
		}

		@Override
		public Exception getException() {
			return null;
		}

		@Override
		public String getFromMember() {
			return null;
		}

	}

}
