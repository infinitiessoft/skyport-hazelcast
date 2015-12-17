package com.infinities.skyport.distributed.impl.hazelcast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastCacheTest {

	private HazelcastCache<String, String> cache;
	// private DistributedLockFactory lockFactory;
	private static HazelcastInstance hazelcastInstance;


	@BeforeClass
	public static void beforeClass() {
		Config config = new Config();
		config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
		config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
		config.getGroupConfig().setName("cacheTast");
		hazelcastInstance = Hazelcast.newHazelcastInstance(config);
	}

	@AfterClass
	public static void afterClass() {
		hazelcastInstance.getLifecycleService().shutdown();

	}

	@Before
	public void setUp() throws Exception {
		// lockFactory = new DistributedLockFactory(new
		// HazelcastDataStructureFactory(hazelcastInstance));
	}

	@After
	public void tearDown() throws Exception {
		if (cache != null) {
			cache.destroy();
		}
		// lockFactory.close();
	}

	@Test(expected = NullPointerException.class)
	public void testHazelcastCacheStringMapOfKVHazelcastInstance() {
		Map<String, String> map = null;
		cache = new HazelcastCache<String, String>("test", map, hazelcastInstance);
	}

	@Test
	public void testHazelcastCacheStringMapOfKVHazelcastInstance2() {
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache = new HazelcastCache<String, String>("test", map, hazelcastInstance);
		assertTrue(cache.entrySet().equals(map.entrySet()));
	}

	@Test(expected = IllegalStateException.class)
	public void testHazelcastCacheStringThrowableHazelcastInstance() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.get("test");
	}

	@Test(expected = IllegalStateException.class)
	public void testHazelcastCacheStringHazelcastInstance() {
		cache = new HazelcastCache<String, String>("test", hazelcastInstance);
		cache.get("test");
	}

	@Test
	public void testReloadMapOfKV() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		assertTrue(cache.entrySet().equals(map.entrySet()));
	}

	@Test(expected = IllegalStateException.class)
	public void testReloadThrowable() {
		cache = new HazelcastCache<String, String>("test", hazelcastInstance);
		Throwable e = new IllegalArgumentException("test");
		cache.reload(e);
		cache.get("test");
	}

	@Test
	public void testClear() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		cache.clear();
		assertEquals(0, cache.size());
	}

	@Test
	public void testClear2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.clear();
	}

	@Test
	public void testContainsKey() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		assertTrue(cache.containsKey("test"));
		assertFalse(cache.containsKey("test3"));
	}

	@Test(expected = IllegalStateException.class)
	public void testContainsKey2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.containsKey("test");
	}

	@Test
	public void testContainsValue() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		assertTrue(cache.containsValue("test"));
		assertFalse(cache.containsValue("test3"));
	}

	@Test(expected = IllegalStateException.class)
	public void testContainsValue2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		assertTrue(cache.containsValue("test"));
	}

	@Test
	public void testEntrySet() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		assertEquals(map.entrySet(), cache.entrySet());
		map.put("test3", "test3");
		assertNotEquals(map.entrySet(), cache.entrySet());
	}

	@Test(expected = IllegalStateException.class)
	public void testEntrySet2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.entrySet();
	}

	@Test
	public void testGet() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		assertEquals("test", cache.get("test"));
	}

	@Test(expected = IllegalStateException.class)
	public void testGet2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.get("test");
	}

	@Test
	public void testIsEmpty() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		assertFalse(cache.isEmpty());
		cache.clear();
		assertTrue(cache.isEmpty());
	}

	@Test(expected = IllegalStateException.class)
	public void testIsEmpty2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.isEmpty();
	}

	@Test
	public void testKeySet() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		assertEquals(map.keySet(), cache.keySet());
		map.put("test3", "test3");
		assertNotEquals(map.keySet(), cache.keySet());
	}

	@Test(expected = IllegalStateException.class)
	public void testKeySet2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.keySet();
	}

	@Test
	public void testPut() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		cache.put("test3", "test3");
		assertTrue(cache.containsKey("test3"));
	}

	@Test(expected = IllegalStateException.class)
	public void testPut2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.put("test3", "test3");
	}

	@Test
	public void testPutAll() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.putAll(map);
		cache.refresh();
		assertEquals(map.entrySet(), cache.entrySet());
	}

	@Test(expected = IllegalStateException.class)
	public void testPutAll3() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.putAll(map);
		assertEquals(map.entrySet(), cache.entrySet());
	}

	@Test(expected = NullPointerException.class)
	public void testPutAll2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = null;
		cache.putAll(map);
	}

	@Test
	public void testRemove() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		assertEquals("test", cache.remove("test"));
	}

	@Test(expected = IllegalStateException.class)
	public void testRemove2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.remove("test");
	}

	@Test
	public void testSize() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		assertEquals(2, cache.size());
	}

	@Test(expected = IllegalStateException.class)
	public void testSize2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.size();
	}

	@Test
	public void testValues() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		assertTrue(map.values().containsAll(cache.values()));
		assertTrue(cache.values().containsAll(map.values()));
		map.put("test3", "test3");
		assertNotEquals(map.values(), cache.values());
	}

	@Test(expected = IllegalStateException.class)
	public void testValues2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.values();
	}

	@Test(expected = IllegalStateException.class)
	public void testDestroy() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		cache.destroy();
		cache.values();
	}

	@Test
	public void testSet() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		cache.set("test3", "test3");
		assertTrue(cache.containsKey("test3"));
	}

	@Test
	public void testSet2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.set("test3", "test3");
	}

	@Test
	public void testDelete() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		Map<String, String> map = new HashMap<String, String>();
		map.put("test", "test");
		map.put("test2", "test2");
		cache.reload(map);
		cache.delete("test");
		assertFalse(cache.containsKey("test3"));
	}

	@Test
	public void testDelete2() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.delete("test");
	}

	@Test
	public void testRefresh() {
		cache = new HazelcastCache<String, String>("test", new IllegalStateException("test"), hazelcastInstance);
		cache.refresh();
		cache.get("test");
	}
}
