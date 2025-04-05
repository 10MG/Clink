package cn.tenmg.clink;

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

import cn.tenmg.clink.config.loader.XMLConfigLoader;
import cn.tenmg.clink.config.model.Clink;
import cn.tenmg.clink.utils.ClassUtils;

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
				List<Callable<Clink>> tasks = Arrays.asList(newParsingTask(), newParsingTask(), newParsingTask(),
						newParsingTask(), newParsingTask(), newParsingTask(), newParsingTask(), newParsingTask());
				List<Future<Clink>> clinks = executorService.invokeAll(tasks, 3000, TimeUnit.MILLISECONDS);
				for (Future<Clink> future : clinks) {
					Clink clink = future.get();
					System.out.println(clink);
				}
				executorService.shutdown();
			}

		});

	}

	private static Callable<Clink> newParsingTask() {
		return new Callable<Clink>() {
			@Override
			public Clink call() throws Exception {
				return XMLConfigLoader.getInstance()
						.load(ClassUtils.getDefaultClassLoader().getResourceAsStream("ParallelParsingTest.xml"));
			}
		};
	}

}
