package cn.tenmg.flink.jobs.kit;

/**
 * 参数配套工具
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.3
 */
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
