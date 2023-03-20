package cn.tenmg.flink.jobs.kit;

/**
 * 参数配套工具。已废弃，请使用 {@code cn.tenmg.dsl.utils.MapUtils} 替换
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.3
 */
@Deprecated
public class ParamsKit extends HashMapKit<String, Object> {
	/**
	 * 初始化参数配套工具，并返回ParamsKit对象
	 * 
	 * @return 返回ParamsKit对象
	 */
	public static ParamsKit init() {
		return new ParamsKit();
	}

}
