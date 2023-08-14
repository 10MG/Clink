package cn.tenmg.clink.clients.utils;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;

/**
 * flink-jobs客户端工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.3.0
 */
public abstract class ClinkClientsUtils {

	private static Class<?> SavepointFormatType;

	static {
		try {
			SavepointFormatType = Class.forName("org.apache.flink.core.execution.SavepointFormatType");
		} catch (ClassNotFoundException e) {
			SavepointFormatType = null;
		}
	}

	public static CompletableFuture<String> stop(ClusterClient<?> client, JobID jobId) throws Exception {
		return stop(client, jobId, null);
	}

	@SuppressWarnings("unchecked")
	public static CompletableFuture<String> stop(ClusterClient<?> client, JobID jobId, String savepointsDir)
			throws Exception {
		Method methods[] = client.getClass().getMethods(), method = null;
		for (int i = 0; i < methods.length; i++) {
			method = methods[i];
			if ("stopWithSavepoint".equals(method.getName())) {
				break;
			}
		}
		if (method != null && "stopWithSavepoint".equals(method.getName())) {
			if (method.getParameterTypes().length == 3) {
				return (CompletableFuture<String>) method.invoke(client, jobId, false, savepointsDir);
			} else {
				return (CompletableFuture<String>) method.invoke(client, jobId, false, savepointsDir,
						SavepointFormatType.getEnumConstants()[0]);
			}
		} else {
			throw new IllegalAccessException(
					"This method does not support your version of Flink yet, please change to the supported version");
		}
	}

}
