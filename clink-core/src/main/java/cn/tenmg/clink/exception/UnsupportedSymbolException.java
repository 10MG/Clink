package cn.tenmg.clink.exception;

/**
 * 不支持符号异常
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class UnsupportedSymbolException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8647336214396104629L;

	public UnsupportedSymbolException() {
		super();
	}

	public UnsupportedSymbolException(String massage) {
		super(massage);
	}

	public UnsupportedSymbolException(Throwable cause) {
		super(cause);
	}

	public UnsupportedSymbolException(String massage, Throwable cause) {
		super(massage, cause);
	}
}
