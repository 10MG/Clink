package cn.tenmg.clink.exception;

/**
 * 不支持类型异常
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class UnsupportedTypeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6218414349783253278L;

	public UnsupportedTypeException() {
		super();
	}

	public UnsupportedTypeException(String massage) {
		super(massage);
	}

	public UnsupportedTypeException(Throwable cause) {
		super(cause);
	}

	public UnsupportedTypeException(String massage, Throwable cause) {
		super(massage, cause);
	}

}
