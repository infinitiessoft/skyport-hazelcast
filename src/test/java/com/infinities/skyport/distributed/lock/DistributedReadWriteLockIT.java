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
package com.infinities.skyport.distributed.lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.infinities.skyport.distributed.impl.hazelcast.HazelcastDataStructureFactory;
import com.infinities.skyport.distributed.lock.DistributedLockUtils;
import com.infinities.skyport.distributed.lock.DistributedReadWriteLockImpl;
import com.infinities.skyport.testcase.IntegrationTest;

/**
 * @author vanessa.williams
 */
@Category(IntegrationTest.class)
public class DistributedReadWriteLockIT extends DistributedLockUtils {

	@BeforeClass
	public static void createGrids() {
		/*
		 * Suppress Hazelcast log output to standard error which does not appear
		 * to be suppressible via Agent Server's log4j.xml.
		 */
		// ConsoleOutputSuppressor.suppressStandardError();
		final Config standardConfig1 = new ClasspathXmlConfig("hazelcast1.xml"), standardConfig2 = new ClasspathXmlConfig(
				"hazelcast2.xml");

		standardConfig1.setProperty("hazelcast.operation.call.timeout.millis", "1000");
		standardConfig2.setProperty("hazelcast.operation.call.timeout.millis", "1000");
		grid1 = Hazelcast.newHazelcastInstance(standardConfig1);
		grid2 = Hazelcast.newHazelcastInstance(standardConfig2);

		dataStructureFactory1 = new HazelcastDataStructureFactory(grid1);
		// dataStructureFactory2 = new HazelcastDataStructureFactory(grid2);

		lockFactory1 = new PublicDistributedLockFactory(dataStructureFactory1);
		// lockFactory2 = new
		// PublicDistributedLockFactory(dataStructureFactory2);
	}

	/**
	 * write-locking and read-locking an unlocked lock succeed
	 */
	@Test
	public void lockingUnlockedSucceeds() {
		final PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock");
		assertNotWriteLocked(lock);
		assertNotReadLocked(lock);

		lock.writeLock().lock();
		assertWriteLocked(lock);
		assertNotReadLocked(lock);

		lock.writeLock().unlock();
		assertNotWriteLocked(lock);
		assertNotReadLocked(lock);

		lock.readLock().lock();
		assertNotWriteLocked(lock);
		assertReadLocked(lock);

		lock.readLock().unlock();
		assertNotWriteLocked(lock);
		assertNotReadLocked(lock);
	}

	/**
	 * write-lockInterruptibly is interruptible Note: you may have to adjust the
	 * property hazelcast.operation.call.timeout.millis to <2000 ms. Default is
	 * 120s!
	 * 
	 * @throws TimeoutException
	 */
	@Test
	public void testWriteLockInterruptibly_Interruptible() throws TimeoutException {
		final PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock");

		lock.writeLock().lock();
		Thread t = newStartedThread(new CheckedInterruptedRunnable() {

			public void realRun() throws InterruptedException {
				lock.writeLock().lockInterruptibly();
			}
		});

		waitForQueuedThread(lock, t);
		t.interrupt();
		awaitTermination(t);
		lock.writeLock().unlock();
	}

	/**
	 * read-lockInterruptibly is interruptible Note: you may have to adjust the
	 * property hazelcast.operation.call.timeout.millis to <2000 ms. Default is
	 * 120s!
	 * 
	 * @throws TimeoutException
	 */
	@Test
	public void testReadLockInterruptibly_Interruptible() throws TimeoutException {
		final PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock");

		lock.writeLock().lock();
		Thread t = newStartedThread(new CheckedInterruptedRunnable() {

			public void realRun() throws InterruptedException {
				lock.readLock().lockInterruptibly();
			}
		});

		waitForQueuedThread(lock, t);
		t.interrupt();
		awaitTermination(t, 10 * LONG_DELAY_MS);
		lock.writeLock().unlock();
	}

	/**
	 * timed try read-lock is interruptible
	 * 
	 * @throws TimeoutException
	 */
	@Test
	public void testTryReadLock_Interruptible() throws TimeoutException {
		final PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock");

		lock.writeLock().lock();
		Thread t = newStartedThread(new CheckedInterruptedRunnable() {

			public void realRun() throws InterruptedException {
				lock.readLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS);
			}
		});

		// waitForQueuedThread(lock, t);
		t.interrupt();
		awaitTermination(t, 2 * LONG_DELAY_MS);
		lock.writeLock().unlock();
	}

	/**
	 * timed try write-lock is interruptible
	 * 
	 * @throws TimeoutException
	 */
	@Test
	public void testTryWriteLock_Interruptible() throws TimeoutException {
		final PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock");

		lock.writeLock().lock();
		Thread t = newStartedThread(new CheckedInterruptedRunnable() {

			public void realRun() throws InterruptedException {
				lock.writeLock().tryLock(2 * LONG_DELAY_MS, TimeUnit.MILLISECONDS);
			}
		});

		// waitForQueuedThread(lock, t);
		t.interrupt();
		awaitTermination(t, 2 * LONG_DELAY_MS);
		lock.writeLock().unlock();
	}

	/**
	 * read-tryLock fails if locked
	 * 
	 * @throws TimeoutException
	 */
	@Test
	public void testReadTryLockWhenLocked() throws TimeoutException {
		final PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock");
		lock.writeLock().lock();
		Thread t = newStartedThread(new CheckedRunnable() {

			public void realRun() {
				assertFalse(lock.readLock().tryLock());
			}
		});

		awaitTermination(t);
		lock.writeLock().unlock();
	}

	/**
	 * write-unlocking an unlocked lock throws IllegalMonitorStateException
	 */
	@Test(expected = IllegalMonitorStateException.class)
	public void testWriteLock_MSIE() {
		final PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock");
		assertNotWriteLocked(lock);
		assertNotReadLocked(lock);

		lock.writeLock().unlock();
	}

	/**
	 * read-unlocking an unlocked lock throws IllegalMonitorStateException
	 */
	@Test(expected = IllegalMonitorStateException.class)
	public void testReadLock_MSIE() {
		PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1.getReadWriteLock("testLock");
		assertNotWriteLocked(lock);
		assertNotReadLocked(lock);

		lock.readLock().unlock();
	}

	/**
	 * read-locking a readlocked lock throws IllegalThreadStateException
	 */
	@Test(expected = IllegalThreadStateException.class)
	public void testTwoNestedReads_ITSE() {
		PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1.getReadWriteLock("testLock");
		assertNotWriteLocked(lock);
		assertNotReadLocked(lock);

		lock.readLock().lock();
		assertReadLocked(lock);
		try {
			lock.readLock().lock();
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * write-locking a writelocked lock throws IllegalThreadStateException
	 */
	@Test(expected = IllegalThreadStateException.class)
	public void testTwoNestedWrites_ITSE() {
		PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1.getReadWriteLock("testLock");
		assertNotWriteLocked(lock);
		assertNotReadLocked(lock);

		lock.writeLock().lock();
		assertWriteLocked(lock);
		try {
			lock.writeLock().lock();
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * upgrading a readlocked lock throws IllegalThreadStateException
	 */
	@Test(expected = IllegalThreadStateException.class)
	public void upgradeFails_ITSE() {
		PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1.getReadWriteLock("testLock");
		assertNotWriteLocked(lock);
		assertNotReadLocked(lock);

		lock.readLock().lock();
		assertReadLocked(lock);
		try {
			lock.writeLock().lock();
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * downgrading a writelocked lock throws IllegalThreadStateException
	 */
	@Test(expected = IllegalThreadStateException.class)
	public void downgradeFails_ITSE() {
		PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1.getReadWriteLock("testLock");
		assertNotWriteLocked(lock);
		assertNotReadLocked(lock);

		lock.writeLock().lock();
		assertWriteLocked(lock);
		try {
			lock.readLock().lock();
		} finally {
			lock.writeLock().unlock();
		}
	}

	/**
	 * Multiple threads can hold a read lock when not write-locked
	 * 
	 * @throws TimeoutException
	 */
	@Test
	public void testMultipleReaders() throws TimeoutException {
		final PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock");

		lock.readLock().lock();
		Thread t = newStartedThread(new CheckedRunnable() {

			public void realRun() throws InterruptedException {
				lock.readLock().lock();
				lock.readLock().unlock();
			}
		});

		awaitTermination(t);
		lock.readLock().unlock();
	}

	/**
	 * A writelock succeeds only after a reading thread unlocks
	 * 
	 * @throws TimeoutException
	 */
	@Test
	public void testWriteAfterReadLock() throws TimeoutException {
		final PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock");

		lock.readLock().lock();
		Thread t = newStartedThread(new CheckedRunnable() {

			public void realRun() {
				assertEquals(1, lock.getReadCount());
				lock.writeLock().lock();
				assertEquals(0, lock.getReadCount());
				lock.writeLock().unlock();
			}
		});
		waitForQueuedThread(lock, t);
		assertNotWriteLocked(lock);
		assertEquals(1, lock.getReadCount());
		lock.readLock().unlock();
		assertEquals(0, lock.getReadCount());
		awaitTermination(t);
		assertNotWriteLocked(lock);

	}

	/**
	 * Readlocks succeed only after a writing thread unlocks
	 * 
	 * @throws TimeoutException
	 */
	@Test
	public void testReadAfterWriteLock() throws TimeoutException {
		final PublicDistributedReadWriteLock lock = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock");

		lock.writeLock().lock();
		Thread t1 = newStartedThread(new CheckedRunnable() {

			public void realRun() {
				lock.readLock().lock();
				lock.readLock().unlock();
			}
		});
		Thread t2 = newStartedThread(new CheckedRunnable() {

			public void realRun() {
				lock.readLock().lock();
				lock.readLock().unlock();
			}
		});

		waitForQueuedThread(lock, t1);
		waitForQueuedThread(lock, t2);
		// releaseWriteLock(lock);
		lock.writeLock().unlock();
		awaitTermination(t1);
		awaitTermination(t2);
	}

	/**
	 * Same thread can get readlocks on multiple named locks.
	 */
	@Test
	public void multipleNamedReadLocks() {
		final PublicDistributedReadWriteLock lock1 = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock1");
		final PublicDistributedReadWriteLock lock2 = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock2");

		lock1.readLock().lock();
		assertReadLocked(lock1);
		assertEquals(1, lock1.getReadCount());
		lock2.readLock().lock();
		assertReadLocked(lock2);
		assertEquals(1, lock2.getReadCount());

		lock1.readLock().unlock();
		assertNotReadLocked(lock1);
		assertEquals(0, lock1.getReadCount());

		lock2.readLock().unlock();
		assertNotReadLocked(lock2);
		assertEquals(0, lock2.getReadCount());
	}

	/**
	 * Same thread can get writeLocks on multiple named locks.
	 */
	@Test
	public void multipleNamedWriteLocks() {
		final PublicDistributedReadWriteLock lock1 = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock1");
		final PublicDistributedReadWriteLock lock2 = (PublicDistributedReadWriteLock) lockFactory1
				.getReadWriteLock("testLock2");

		lock1.writeLock().lock();
		assertWriteLocked(lock1);
		assertEquals(1, lock1.getWriteCount());
		lock2.writeLock().lock();
		assertWriteLocked(lock2);
		assertEquals(1, lock2.getWriteCount());

		lock1.writeLock().unlock();
		assertNotWriteLocked(lock1);
		assertEquals(0, lock1.getWriteCount());

		lock2.writeLock().unlock();
		assertNotWriteLocked(lock2);
		assertEquals(0, lock2.getWriteCount());

	}

	@AfterClass
	public static void tearDown() {
		grid1.shutdown();
		grid2.shutdown();
	}

	public void assertNotWriteLocked(DistributedReadWriteLockImpl lock) {
		assertFalse(((PublicDistributedReadWriteLock) lock).isWriteLocked());
	}

	public void assertWriteLocked(DistributedReadWriteLockImpl lock) {
		assertTrue(((PublicDistributedReadWriteLock) lock).isWriteLocked());
	}

	public void assertReadLocked(DistributedReadWriteLockImpl lock) {
		assertTrue(((PublicDistributedReadWriteLock) lock).isReadLocked());
	}

	public void assertNotReadLocked(DistributedReadWriteLockImpl lock) {
		assertFalse(((PublicDistributedReadWriteLock) lock).isReadLocked());
	}


	private static HazelcastInstance grid1, grid2;
	private static HazelcastDataStructureFactory dataStructureFactory1; // ,
																		// dataStructureFactory2;
	private static PublicDistributedLockFactory lockFactory1;// , lockFactory2;

}
