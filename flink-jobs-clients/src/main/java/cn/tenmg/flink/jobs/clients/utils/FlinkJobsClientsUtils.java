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
		Method method;
		if (SavepointFormatType == null) {
			method = client.getClass().getMethod("stopWithSavepoint", JobID.class, Boolean.class, String.class);
			return (CompletableFuture<String>) method.invoke(client, jobId, false,
					FlinkJobsClientsContext.getProperty("state.savepoints.dir"));
		} else {
			method = client.getClass().getMethod("stopWithSavepoint", JobID.class, Boolean.class, String.class,
					SavepointFormatType);
			return (CompletableFuture<String>) method.invoke(client, jobId, false,
					FlinkJobsClientsContext.getProperty("state.savepoints.dir"),
					SavepointFormatType.getEnumConstants()[0]);
		}
	}

}
