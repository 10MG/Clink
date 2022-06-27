package cn.tenmg.flink.jobs.clients.utils;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;

import cn.tenmg.flink.jobs.clients.context.FlinkJobsClientsContext;

public abstract class FlinkJobsClientsUtils {

	private static Class<?> SavepointFormatType;

	static {
		try {
			SavepointFormatType = Class.forName("org.apache.flink.core.execution.SavepointFormatType");
		} catch (ClassNotFoundException e) {
			SavepointFormatType = null;
		}
	}

	@SuppressWarnings("unchecked")
	public static CompletableFuture<String> stop(ClusterClient<?> client, JobID jobId) throws Exception {
		Method methods[] = client.getClass().getMethods(), method = null;
		for (int i = 0; i < methods.length; i++) {
			method = methods[i];
			if ("stopWithSavepoint".equals(method.getName())) {
				break;
			}
		}
		if (method != null && "stopWithSavepoint".equals(method.getName())) {
			if (method.getParameterTypes().length == 3) {
				return (CompletableFuture<String>) method.invoke(client, jobId, false,
						FlinkJobsClientsContext.getProperty("state.savepoints.dir"));
			} else {
				return (CompletableFuture<String>) method.invoke(client, jobId, false,
						FlinkJobsClientsContext.getProperty("state.savepoints.dir"),
						SavepointFormatType.getEnumConstants()[0]);
			}
		} else {
			throw new IllegalAccessException(
					"This method does not support your version of Flink yet, please change the supported version");
		}
	}

}
