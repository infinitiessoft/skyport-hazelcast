package com.infinities.skyport.distributed.impl.hazelcast.hazeltask.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

//@Category(IntegrationTest.class)
public class SimpleDistributedFutureTest {

	protected Mockery context = new JUnit4Mockery() {

		{
			setThreadingPolicy(new Synchroniser());
			setImposteriser(ClassImposteriser.INSTANCE);
		}
	};
	private SimpleDistributedFuture<String> f;
	private IExecutorTopologyService service;
	private String id;


	@Before
	public void setUp() throws Exception {
		id = UUID.randomUUID().toString();
		service = context.mock(IExecutorTopologyService.class);
		f = new SimpleDistributedFuture<String>(service, id);
	}

	@After
	public void tearDown() throws Exception {
		f.set("done");
		f = null;
	}

	@Test(expected = CancellationException.class)
	public void testCancel() throws InterruptedException, ExecutionException {
		context.checking(new Expectations() {

			{
				oneOf(service).cancelTask(id);
				will(returnValue(true));
			}
		});
		assertFalse(f.isDone());
		assertTrue(f.cancel(true));
		assertTrue(f.isDone());
		assertTrue(f.isCancelled());
		f.get();
	}

	@Test(expected = CancellationException.class)
	public void testCancel3() throws InterruptedException, ExecutionException {
		context.checking(new Expectations() {

			{
				oneOf(service).cancelTask(id);
				will(returnValue(true));
			}
		});
		assertFalse(f.isDone());
		assertTrue(f.cancel(false));
		assertTrue(f.isDone());
		assertTrue(f.isCancelled());
		f.get();
	}

	@Test(expected = TimeoutException.class)
	public void testCancel2() throws InterruptedException, ExecutionException, TimeoutException {
		context.checking(new Expectations() {

			{
				oneOf(service).cancelTask(id);
				will(returnValue(false));
			}
		});
		assertFalse(f.isDone());
		assertFalse(f.cancel(true));
		assertFalse(f.isDone());
		assertFalse(f.isCancelled());
		f.get(10, TimeUnit.MILLISECONDS);
	}

	@Test(expected = TimeoutException.class)
	public void testCancel4() throws InterruptedException, ExecutionException, TimeoutException {
		context.checking(new Expectations() {

			{
				oneOf(service).cancelTask(id);
				will(returnValue(false));
			}
		});
		assertFalse(f.isDone());
		assertFalse(f.cancel(false));
		assertFalse(f.isDone());
		assertFalse(f.isCancelled());
		f.get(10, TimeUnit.MILLISECONDS);
	}

	@Test(expected = CancellationException.class)
	public void testSetCancelled() throws InterruptedException, ExecutionException {
		f = new SimpleDistributedFuture<String>(service, id);
		assertFalse(f.isDone());
		assertTrue(f.setCancelled(true));
		assertTrue(f.isDone());
		assertTrue(f.isCancelled());
		f.get();
	}

	@Test(expected = CancellationException.class)
	public void testSetCancelled2() throws InterruptedException, ExecutionException {
		assertFalse(f.isDone());
		assertTrue(f.setCancelled(false));
		assertTrue(f.isDone());
		assertTrue(f.isCancelled());
		f.get();

	}

	@Test(expected = ExecutionException.class)
	public void testSetExceptionThrowable() throws InterruptedException, ExecutionException {
		f.setException(new IllegalStateException("test"));
		f.get();
	}

	@Test
	public void testSetT() {
		assertFalse(f.isDone());
		f.set("test");
		assertTrue(f.isDone());
	}

	@Test(expected = TimeoutException.class)
	public void testGetTimeout() throws InterruptedException, TimeoutException, ExecutionException {
		f.get(10, TimeUnit.MILLISECONDS);
	}

	@Test(timeout = 300)
	public void testGetWithThreadSet() throws InterruptedException, TimeoutException, ExecutionException {
		Thread t = new Thread() {

			@Override
			public void run() {
				try {
					Thread.sleep(100);
					f.set("Hello");
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		};
		t.start();
		assertEquals("Hello", f.get());
	}

	@Test
	public void testListenableFuture() throws InterruptedException {
		final AtomicReference<String> ref = new AtomicReference<String>();
		Futures.addCallback(f, new FutureCallback<String>() {

			@Override
			public void onSuccess(String result) {
				ref.set(result);
			}

			@Override
			public void onFailure(Throwable t) {
				throw new RuntimeException(t);
			}
		});

		Thread t = new Thread() {

			@Override
			public void run() {
				f.set("Hello");
			}
		};
		t.start();

		Thread.sleep(100);
		Assert.assertEquals("Hello", ref.get());
	}

	@Test
	public void testListenableFutureException() throws InterruptedException {
		final AtomicReference<Boolean> ref = new AtomicReference<Boolean>();
		Futures.addCallback(f, new FutureCallback<String>() {

			@Override
			public void onSuccess(String result) {

			}

			@Override
			public void onFailure(Throwable t) {
				ref.set(t instanceof IllegalStateException);
			}
		});

		Thread t = new Thread() {

			@Override
			public void run() {
				f.setException(new IllegalStateException("test"));
			}
		};
		t.start();

		Thread.sleep(100);
		Assert.assertTrue(ref.get());
	}

	@Test
	public void testGetTaskId() {
		assertEquals(id, f.getTaskId());
	}

	@Test
	public void testGetCreatedTime() throws InterruptedException {
		Thread.sleep(10);
		long now = System.currentTimeMillis();
		SimpleDistributedFuture<String> f = new SimpleDistributedFuture<String>(service, id);
		assertTrue(timeBuffer(now, (long) f.getCreatedTime()));
	}

	private boolean timeBuffer(long expected, long actual) {
		long buffer = 2;
		return actual <= (expected + buffer) && actual >= (expected - buffer);
	}

}
