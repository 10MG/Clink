package cn.tenmg.clink.exception;

/**
 * 非法任务配置异常
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class IllegalJobConfigException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6297483758419615943L;

	public IllegalJobConfigException() {
		super();
	}

	public IllegalJobConfigException(String massage) {
		super(massage);
	}

	public IllegalJobConfigException(Throwable cause) {
		super(cause);
	}

	public IllegalJobConfigException(String massage, Throwable cause) {
		super(massage, cause);
	}

}
