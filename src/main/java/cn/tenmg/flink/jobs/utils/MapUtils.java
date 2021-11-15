package cn.tenmg.flink.jobs.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 查找表工具类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public abstract class MapUtils {

	/**
	 * 新建哈希查找表并将指定查找表中的元素全部放入后返回
	 * 
	 * @param map
	 *            指定查找表
	 * @return 返回含有指定查找表元素的新的哈希查找表
	 */
	public static <K, V> HashMap<K, V> newHashMap(Map<K, V> map) {
		HashMap<K, V> newHashMap = new HashMap<K, V>();
		newHashMap.putAll(map);
		return newHashMap;
	}

	/**
	 * 使用键集合移除指定查找表中的元素
	 * 
	 * @param map
	 *            指定查找表
	 * @param keys
	 *            键集合
	 */
	public static void removeAll(Map<String, String> map, Set<String> keys) {
		for (Iterator<String> it = keys.iterator(); it.hasNext();) {
			map.remove(it.next());
		}
	}
}
