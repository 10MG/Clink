package cn.tenmg.flink.jobs.kit;

import java.util.HashMap;
import java.util.Map;

/**
 * HashMap配套工具。已废弃，请使用 {@code cn.tenmg.dsl.utils.MapUtils} 替换
 * 
 * @param <K>
 *            键的类型
 * @param <V>
 *            值的类型
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.3
 */
@Deprecated
public class HashMapKit<K, V> {

	private HashMap<K, V> hashMap = new HashMap<K, V>();

	/**
	 * 初始化只有一个键值对的HashMap并返回
	 * 
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @return 返回只有一个键值对的HashMap对象
	 */
	public static <K, V> HashMap<K, V> single(K key, V value) {
		HashMap<K, V> hashMap = new HashMap<K, V>();
		hashMap.put(key, value);
		return hashMap;
	}

	/**
	 * 初始化HashMap配套工具，将键值存入后哈希查找表中，并返回HashMapKit对象
	 * 
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @return 返回HashMapKit对象
	 */
	public static <K, V> HashMapKit<K, V> init(K key, V value) {
		HashMapKit<K, V> kit = new HashMapKit<K, V>();
		kit.put(key, value);
		return kit;
	}

	/**
	 * 初始化HashMap配套工具，将键值存入后哈希查找表中，并返回HashMapKit对象
	 * 
	 * @param map
	 *            查找表对象
	 * @return 返回HashMapKit对象
	 */
	public static <K, V> HashMapKit<K, V> init(Map<K, V> map) {
		HashMapKit<K, V> kit = new HashMapKit<K, V>();
		kit.put(map);
		return kit;
	}

	/**
	 * 将键、值存入希查找表中
	 * 
	 * @param map
	 *            指定查找表
	 * @return 返回HashMapKit对象
	 */
	public HashMapKit<K, V> put(Map<K, V> map) {
		if (map != null) {
			hashMap.putAll(map);
		}
		return this;
	}

	/**
	 * 将指定查找表中的元素全部放入哈希表中
	 * 
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @return 返回HashMapKit对象
	 */
	public HashMapKit<K, V> put(K key, V value) {
		hashMap.put(key, value);
		return this;
	}

	/**
	 * 返回创建的HashMap对象
	 * 
	 * @return 返回HashMap对象
	 */
	public HashMap<K, V> get() {
		return hashMap;
	}

}
