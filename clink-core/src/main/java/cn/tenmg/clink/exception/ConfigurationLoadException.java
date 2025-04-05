package cn.tenmg.clink.exception;

/**
 * 配置加载异常
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public class ConfigurationLoadException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1134926772872317537L;

	public ConfigurationLoadException() {
		super();
	}

	public ConfigurationLoadException(String massage) {
		super(massage);
	}

	public ConfigurationLoadException(Throwable cause) {
		super(cause);
	}

	public ConfigurationLoadException(String massage, Throwable cause) {
		super(massage, cause);
	}
}
