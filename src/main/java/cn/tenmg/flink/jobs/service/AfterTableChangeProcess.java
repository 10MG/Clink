package cn.tenmg.flink.jobs.service;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import cn.tenmg.flink.jobs.model.TableOperate;

/**
 * 数据库表数据变更处理服务的后置服务
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 */
public interface AfterTableChangeProcess extends Serializable {

	/**
	 * 记录日志
	 * 
	 * @param tableOperates
	 * @param serviceName
	 * @param startTime
	 * @param endTime
	 * @param success
	 * @param msg
	 */
	void log(List<TableOperate> tableOperates, String serviceName, Date startTime, Date endTime, boolean success,
			String msg);

	/**
	 * 记录最新的偏移量
	 * 
	 * @param tableOperate
	 *            最新表数据变更消息
	 * @param serviceName
	 *            服务名称
	 */
	void saveStartingOffsets(TableOperate tableOperate, String serviceName);

}