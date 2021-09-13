package cn.tenmg.flink.jobs.utils;

import javax.swing.Spring;

/**
 * 类工具类。请使用cn.tenmg.dsl.utils.ClassUtils替换
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * @see Spring
 * 
 * @since 1.1.0
 * 
 */
@Deprecated
public class ClassUtils {
	/**
	 * Return the default ClassLoader to use: typically the thread context
	 * ClassLoader, if available; the ClassLoader that loaded the ClassUtils class
	 * will be used as fallback.
	 * <p>
	 * Call this method if you intend to use the thread context ClassLoader in a
	 * scenario where you clearly prefer a non-null ClassLoader reference: for
	 * example, for class path resource loading (but not necessarily for
	 * {@code Class.forName}, which accepts a {@code null} ClassLoader reference as
	 * well).
	 * 
	 * @return the default ClassLoader (only {@code null} if even the system
	 *         ClassLoader isn't accessible)
	 * @see Thread#getContextClassLoader()
	 * @see ClassLoader#getSystemClassLoader()
	 */
	public static ClassLoader getDefaultClassLoader() {
		ClassLoader cl = null;
		try {
			cl = Thread.currentThread().getContextClassLoader();
		} catch (Throwable ex) {
			// Cannot access thread context ClassLoader - falling back...
		}
		if (cl == null) {
			// No thread context class loader -> use class loader of this class.
			cl = ClassUtils.class.getClassLoader();
			if (cl == null) {
				// getClassLoader() returning null indicates the bootstrap
				// ClassLoader
				try {
					cl = ClassLoader.getSystemClassLoader();
				} catch (Throwable ex) {
					// Cannot access system ClassLoader - oh well, maybe the
					// caller can live with null...
				}
			}
		}
		return cl;
	}
}
