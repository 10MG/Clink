package cn.tenmg.flink.jobs.jdbc.getter;

import java.lang.reflect.ParameterizedType;

import cn.tenmg.flink.jobs.jdbc.ResultGetter;

/**
 * 结果获取器抽象类
 * 
 * @author June wjzhao@aliyun.com
 *
 * @param <T>
 *            结果类型
 * 
 * @since 1.5.6
 */
public abstract class AbstractResultGetter<T> implements ResultGetter<T> {

	protected Class<T> type;

	@SuppressWarnings("unchecked")
	protected AbstractResultGetter() {
		type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	}

	@Override
	public Class<T> getType() {
		return type;
	}

}
