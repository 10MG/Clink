package cn.tenmg.flink.jobs.kit;

import java.util.Map;

/**
 * 参数配套工具
 * 
 * @author 赵伟均 wjzhao@aliyun.com
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

	/**
	 * 初始化参数配套工具，并将指定参数查找表的元素存入哈希查找表中，并返回ParamsKit对象
	 * 
	 * @param params
	 *            参数查找表
	 * @return 返回ParamsKit对象
	 */
	public static ParamsKit init(Map<String, Object> params) {
		ParamsKit kit = init();
		if (params != null) {
			kit.put(params);
		}
		return kit;
	}
}
