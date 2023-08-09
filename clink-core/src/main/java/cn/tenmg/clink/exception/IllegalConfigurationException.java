package cn.tenmg.clink.exception;

/**
 * 非法配置异常
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.1.3
 */
public class IllegalConfigurationException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -611234676646111464L;

	public IllegalConfigurationException() {
		super();
	}

	public IllegalConfigurationException(String massage) {
		super(massage);
	}

	public IllegalConfigurationException(Throwable cause) {
		super(cause);
	}

	public IllegalConfigurationException(String massage, Throwable cause) {
		super(massage, cause);
	}

}
