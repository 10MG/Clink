package cn.tenmg.clink.jdbc.exception;

/**
 * SQL执行异常。通过反射访问或设置属性引发异常时会抛出次异常
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.4.0
 */
public class SQLExecutorException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5523191114712061655L;

	public SQLExecutorException() {
		super();
	}

	public SQLExecutorException(String massage) {
		super(massage);
	}

	public SQLExecutorException(Throwable cause) {
		super(cause);
	}

	public SQLExecutorException(String massage, Throwable cause) {
		super(massage, cause);
	}
}
