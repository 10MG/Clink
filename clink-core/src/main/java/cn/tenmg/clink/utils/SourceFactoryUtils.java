package cn.tenmg.clink.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import cn.tenmg.clink.source.SourceFactory;
import cn.tenmg.dsl.utils.MapUtils;

/**
 * 支持多表的源工厂工具类
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年9月5日
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class SourceFactoryUtils {

	private static volatile Map<String, SourceFactory<Source<Tuple2<String, Row>, ?, ?>>> factories = MapUtils
			.newHashMap();

	static {
		SourceFactory factory;
		ServiceLoader<SourceFactory> loader = ServiceLoader.load(SourceFactory.class);
		for (Iterator<SourceFactory> it = loader.iterator(); it.hasNext();) {
			factory = it.next();
			factories.put(factory.factoryIdentifier(), factory);
		}
	}

	public static SourceFactory<Source<Tuple2<String, Row>, ?, ?>> getSourceFactory(String connector) {
		return factories.get(connector);
	}
}
