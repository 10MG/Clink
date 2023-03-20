package cn.tenmg.flink.jobs;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import cn.tenmg.flink.jobs.config.loader.XMLConfigLoader;
import cn.tenmg.flink.jobs.config.model.FlinkJobs;
import cn.tenmg.flink.jobs.utils.ClassUtils;

/**
 * 并行解析测试
 * 
 * @author June wjzhao@aliyun.com
 * @since 1.5.6
 */
public class ParallelParsingTest {

	@Test
	void parallelParsing() throws Exception {

		Assertions.assertDoesNotThrow(new Executable() {

			@Override
			public void execute() throws Throwable {
				ExecutorService executorService = Executors.newCachedThreadPool();
				List<Callable<FlinkJobs>> tasks = Arrays.asList(newParsingTask(), newParsingTask(), newParsingTask(),
						newParsingTask(), newParsingTask(), newParsingTask(), newParsingTask(), newParsingTask());
				List<Future<FlinkJobs>> flinkJobses = executorService.invokeAll(tasks, 3000, TimeUnit.MILLISECONDS);
				for (Future<FlinkJobs> future : flinkJobses) {
					FlinkJobs flinkJobs = future.get();
					System.out.println(flinkJobs);
				}
				executorService.shutdown();
			}

		});

	}

	private static Callable<FlinkJobs> newParsingTask() {
		return new Callable<FlinkJobs>() {
			@Override
			public FlinkJobs call() throws Exception {
				return XMLConfigLoader.getInstance()
						.load(ClassUtils.getDefaultClassLoader().getResourceAsStream("ParallelParsingTest.xml"));
			}
		};
	}

}
