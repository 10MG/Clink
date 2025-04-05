package cn.tenmg.clink.exception;

/**
 * 数据源未找到异常
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.1.0
 */
public class DataSourceNotFoundException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8684977502348790296L;

	public DataSourceNotFoundException() {
		super();
	}

	public DataSourceNotFoundException(String massage) {
		super(massage);
	}

	public DataSourceNotFoundException(Throwable cause) {
		super(cause);
	}

	public DataSourceNotFoundException(String massage, Throwable cause) {
		super(massage, cause);
	}

}
