package cn.tenmg.flink.jobs.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据表工具类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 */
public abstract class TableUtils {

	public static final String fullTableName(String database, String table) {
		return database.concat(".").concat(table);
	}

	public static final String[] fullTableNames(String database, String[] tables) {
		String[] tableNames = new String[tables.length];
		for (int i = 0; i < tableNames.length; i++) {
			tableNames[i] = fullTableName(database, tables[i]);
		}
		return tableNames;
	}

	public static final String[] fullTableNames(String[] databases, String[][] tables) {
		List<String> tableNames = new ArrayList<String>();
		for (int i = 0; i < databases.length; i++) {
			String database = databases[i];
			String[] table = tables[i];
			for (int j = 0; j < table.length; j++) {
				tableNames.add(fullTableName(database, table[j]));
			}
		}
		return tableNames.toArray(new String[tableNames.size()]);
	}
}
