package cn.tenmg.flink.jobs.clients.utils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * 集合工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 */
public abstract class Sets {

	@SuppressWarnings("unchecked")
	public static final <T> Set<T> as(T... objects) {
		Set<T> set = new HashSet<T>();
		for (int i = 0; i < objects.length; i++) {
			set.add(objects[i]);
		}
		return set;
	}

	public static final <T> Set<T> as(Collection<T> objects) {
		Set<T> set = new HashSet<T>();
		set.addAll(objects);
		return set;
	}

}