package cn.tenmg.flink.jobs.kit;

import java.util.HashMap;
import java.util.Map;

/**
 * HashMap配套工具
 * 
 * @param <K>
 *            键的类型
 * @param <V>
 *            值的类型
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.3
 */
public class HashMapKit<K, V> {

	private HashMap<K, V> hashMap = new HashMap<K, V>();

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
		return new HashMapKit<K, V>();
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
