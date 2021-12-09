package cn.tenmg.flink.jobs.config.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import cn.tenmg.flink.jobs.config.model.params.Param;

/**
 * flink-jobs应用程序启动配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = FlinkJobs.NAMESPACE, name = "flink-jobs")
public class FlinkJobs implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2783184277263685612L;

	/**
	 * 可扩展标记语言（XML）模式定义（Schemas Definition）文件的命名空间
	 */
	public static final String NAMESPACE = "http://www.10mg.cn/schema/flink-jobs";

	@XmlAttribute
	private String jar;

	@XmlAttribute(name = "class")
	private String mainClass;

	@XmlElement(namespace = NAMESPACE)
	@XmlJavaTypeAdapter(OptionsAdapter.class)
	private Options options;

	@XmlAttribute
	private String serviceName;

	@XmlAttribute
	private String runtimeMode;

	@XmlAttribute
	private boolean allwaysNewJob;

	@XmlElement(namespace = NAMESPACE)
	@XmlJavaTypeAdapter(ParamsAdapter.class)
	private HashMap<String, Object> params;

	@XmlElements({ @XmlElement(name = "bsh", type = Bsh.class, namespace = NAMESPACE),
			@XmlElement(name = "execute-sql", type = ExecuteSql.class, namespace = NAMESPACE),
			@XmlElement(name = "sql-query", type = SqlQuery.class, namespace = NAMESPACE),
			@XmlElement(name = "jdbc", type = Jdbc.class, namespace = NAMESPACE),
			@XmlElement(name = "data-sync", type = DataSync.class, namespace = NAMESPACE) })
	private List<Operate> operates;

	/**
	 * 获取运行的JAR包路径
	 * 
	 * @return 运行的JAR包路径
	 */
	public String getJar() {
		return jar;
	}

	/**
	 * 设置运行的JAR包路径
	 * 
	 * @param jar
	 *            运行的JAR包路径
	 */
	public void setJar(String jar) {
		this.jar = jar;
	}

	/**
	 * 获取运行的主类名
	 * 
	 * @return 运行的主类名
	 */
	public String getMainClass() {
		return mainClass;
	}

	/**
	 * 设置运行的主类名，可缺省
	 * 
	 * @param mainClass
	 *            运行的主类名
	 */
	public void setMainClass(String mainClass) {
		this.mainClass = mainClass;
	}

	/**
	 * 获取运行选项
	 * 
	 * @return 运行选项
	 */
	public Options getOptions() {
		return options;
	}

	/**
	 * 设置运行选项
	 * 
	 * @param options
	 *            运行选项
	 */
	public void setOptions(Options options) {
		this.options = options;
	}

	/**
	 * 获取运行的服务名称，可缺省
	 * 
	 * @return 运行的服务名称
	 */
	public String getServiceName() {
		return serviceName;
	}

	/**
	 * 设置运行的服务名称
	 * 
	 * @param serviceName
	 *            运行的服务名称
	 */
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	/**
	 * 判断是否总是作为新作业提交
	 * 
	 * @return 是否总是作为新作业提交
	 */
	public boolean isAllwaysNewJob() {
		return allwaysNewJob;
	}

	/**
	 * 设置是否总是作为新作业提交
	 * 
	 * @param allwaysNewJob
	 *            是否总是作为新作业提交
	 */
	public void setAllwaysNewJob(boolean allwaysNewJob) {
		this.allwaysNewJob = allwaysNewJob;
	}

	/**
	 * 获取运行模式，可缺省
	 * 
	 * @return 运行模式
	 */
	public String getRuntimeMode() {
		return runtimeMode;
	}

	/**
	 * 设置运行模式
	 * 
	 * @param runtimeMode
	 *            运行模式
	 */
	public void setRuntimeMode(String runtimeMode) {
		this.runtimeMode = runtimeMode;
	}

	/**
	 * 获取参数查找表
	 * 
	 * @return 参数查找表
	 */
	public HashMap<String, Object> getParams() {
		return params;
	}

	/**
	 * 设置参数查找表
	 * 
	 * @param params
	 *            参数查找表
	 */
	public void setParams(HashMap<String, Object> params) {
		this.params = params;
	}

	/**
	 * 获取操作列表
	 * 
	 * @return 操作列表
	 */
	public List<Operate> getOperates() {
		return operates;
	}

	/**
	 * 设置操作列表
	 * 
	 * @param operates
	 *            操作列表
	 */
	public void setOperates(List<Operate> operates) {
		this.operates = operates;
	}

	/**
	 * 参数集解析适配器
	 * 
	 * @author June wjzhao@aliyun.com
	 *
	 * @since 1.1.4
	 */
	public static class ParamsAdapter extends XmlAdapter<Params, HashMap<String, Object>> {

		@Override
		public Params marshal(HashMap<String, Object> hashMap) throws Exception {
			Params params = new Params();
			for (Map.Entry<String, Object> mapEntry : hashMap.entrySet()) {
				Param param = new Param();
				param.setName(mapEntry.getKey());
				param.setValue(mapEntry.getValue().toString());
			}
			return params;
		}

		@Override
		public HashMap<String, Object> unmarshal(Params params) throws Exception {
			HashMap<String, Object> hashMap = new HashMap<String, Object>();
			for (Param param : params.getParam()) {
				hashMap.put(param.getName(), param.getValue());
			}
			return hashMap;
		}

	}

	/**
	 * 原始参数集配置
	 * 
	 * @author June wjzhao@aliyun.com
	 * 
	 * @since 1.1.4
	 */
	@XmlAccessorType(XmlAccessType.FIELD)
	public static class OptionsOriginal {

		@XmlAttribute
		private String keyPrefix = "--";

		@XmlElement(namespace = FlinkJobs.NAMESPACE)
		private List<Option> option;

		public String getKeyPrefix() {
			return keyPrefix;
		}

		public void setKeyPrefix(String keyPrefix) {
			this.keyPrefix = keyPrefix;
		}

		public List<Option> getOption() {
			return option;
		}

		public void setOption(List<Option> option) {
			this.option = option;
		}

	}

	/**
	 * 参数集解析适配器
	 * 
	 * @author June wjzhao@aliyun.com
	 *
	 * @since 1.1.4
	 */
	public static class OptionsAdapter extends XmlAdapter<OptionsOriginal, Options> {

		@Override
		public OptionsOriginal marshal(Options options) throws Exception {
			OptionsOriginal optionsOriginal = new OptionsOriginal();
			optionsOriginal.setKeyPrefix(options.getKeyPrefix());
			Map<String, String> option = options.getOption();
			if (option != null) {
				List<Option> optionList = new ArrayList<Option>();
				for (Map.Entry<String, String> mapEntry : option.entrySet()) {
					optionList.add(new Option(mapEntry.getKey(), mapEntry.getValue()));
				}
				optionsOriginal.setOption(optionList);
			}
			return optionsOriginal;
		}

		@Override
		public Options unmarshal(OptionsOriginal optionsOriginal) throws Exception {
			Options options = new Options();
			options.setKeyPrefix(optionsOriginal.getKeyPrefix());
			List<Option> optionList = optionsOriginal.getOption();
			if (optionList != null) {
				Map<String, String> map = new LinkedHashMap<String, String>();
				for (int i = 0, size = optionList.size(); i < size; i++) {
					Option option = optionList.get(i);
					map.put(option.getKey(), option.getValue());
				}
				options.setOption(map);
			}
			return options;
		}

	}

}
