package cn.tenmg.flink.jobs.operator;

import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.model.CreateTable;
import cn.tenmg.flink.jobs.utils.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CreateTableOperator extends AbstractSqlOperator<CreateTable>{

    private static Logger logger = LoggerFactory.getLogger(CreateTableOperator.class);

    private static final String TABLE_NAME = "table-name",
    /**
     * 删除语句正则表达式
     */
    DELETE_CLAUSE_REGEX = "[\\s]*[D|d][E|e][L|l][E|e][T|t][E|e][\\s]+[F|f][R|r][O|o][M|m][\\s]+[\\S]+",

    /**
     * 更新语句正则表达式
     */
    UPDATE_CLAUSE_REGEX = "[\\s]*[U|u][P|p][D|d][A|a][T|t][E|e][\\s]+[\\S]+[\\s]+[S|s][E|e][T|t][\\s]+[\\S]+";

    private static final Pattern WITH_CLAUSE_PATTERN = Pattern
            .compile("[W|w][I|i][T|t][H|h][\\s]*\\([\\s\\S]*\\)[\\s]*$"),
            CREATE_CLAUSE_PATTERN = Pattern
                    .compile("[C|c][R|r][E|e][A|a][T|t][E|e][\\s]+[T|t][A|a][B|b][L|l][E|e][\\s]+[^\\s\\(]+");

    @Override
    Object execute(StreamTableEnvironment tableEnv, CreateTable operate, Map<String, Object> params) throws Exception {

        String dataSourceStr = operate.getDataSource();
        String tableName = operate.getTableName();
        Map<String, String> dataSource = FlinkJobsContext.getDatasource(dataSourceStr);

        Connection con = null;
        PreparedStatement ps = null;

        try {
            List<Map<String, Object>> rows = getTableColumnsMetaData(dataSource, "falcon", tableName);
            StringBuilder sb = new StringBuilder("Create table " + tableName + "(\n");
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> map = rows.get(i);
                sb.append(map.get("COLUMN_NAME") + " " + convertDataType(map) + ",\n");
            }

            String primaryKeyStr = String.join(",", rows.stream().filter(x -> "PRI".equals(x.get("COLUMN_KEY"))).map(x -> x.get("COLUMN_NAME").toString()).collect(Collectors.toList()));
            if (primaryKeyStr != null) {
                sb.append("PRIMARY KEY(" + primaryKeyStr + ") NOT ENFORCED \n)");
            } else {
                sb.append(")\n");
            }


            logger.info("generated create table statement: " + sb.toString());

            String statement = wrapDataSource(sb.toString(), dataSource);

            return tableEnv.executeSql(statement);
        } catch (Exception e) {
            throw e;
        } finally {
            JDBCUtils.close(ps);
            JDBCUtils.close(con);
        }

    }

    private static String wrapDataSource(String script, Map<String, String> dataSource) throws IOException {
        Matcher matcher = WITH_CLAUSE_PATTERN.matcher(script);
        StringBuffer sqlBuffer = new StringBuffer();
        if (matcher.find()) {
            String group = matcher.group();
            int startIndex = group.indexOf("(") + 1, endIndex = group.lastIndexOf(")");
            String start = group.substring(0, startIndex), value = group.substring(startIndex, endIndex),
                    end = group.substring(endIndex);
            if (StringUtils.isBlank(value)) {
                matcher.appendReplacement(sqlBuffer, start);
                SQLUtils.appendDataSource(sqlBuffer, dataSource);
                if (!dataSource.containsKey(TABLE_NAME)) {
                    apppendDefaultTableName(sqlBuffer, script);
                }
                sqlBuffer.append(end);
            } else {
                Map<String, String> config = ConfigurationUtils.load(value),
                        actualDataSource = MapUtils.newHashMap(dataSource);
                MapUtils.removeAll(actualDataSource, config.keySet());
                matcher.appendReplacement(sqlBuffer, start);
                StringBuilder blank = new StringBuilder();
                int len = value.length(), i = len - 1;
                while (i > 0) {
                    char c = value.charAt(i);
                    if (c > DSLUtils.BLANK_SPACE) {
                        break;
                    }
                    blank.append(c);
                    i--;
                }
                sqlBuffer.append(value.substring(0, i + 1)).append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE);
                SQLUtils.appendDataSource(sqlBuffer, actualDataSource);
                if (ConfigurationUtils.isJDBC(actualDataSource) && !config.containsKey(TABLE_NAME)
                        && !actualDataSource.containsKey(TABLE_NAME)) {
                    apppendDefaultTableName(sqlBuffer, script);
                }
                sqlBuffer.append(blank.reverse()).append(end);
            }
        } else {
            sqlBuffer.append(script);
            sqlBuffer.append(" WITH (");
            SQLUtils.appendDataSource(sqlBuffer, dataSource);
            if (!dataSource.containsKey(TABLE_NAME)) {
                apppendDefaultTableName(sqlBuffer, script);
            }
            sqlBuffer.append(")");
        }
        return sqlBuffer.toString();
    }

    private List<Map<String, Object>> getTableColumnsMetaData(Map<String, String> dataSource, String database, String table) {
        final String query = "select `COLUMN_NAME`, `COLUMN_KEY`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` from `information_schema`.`COLUMNS` where `TABLE_SCHEMA`=? and `TABLE_NAME`=?";
        List<Map<String, Object>> rows;
        logger.info("database: " + database);
        logger.info("table: " + table);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Executing query '%s'", query));
            }
            rows = executeQuery(query, dataSource, database, table);
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException("Failed to get table schema info from StarRocks. " + se.getMessage(), se);
        }
        return rows;
    }

    private List<Map<String, Object>> executeQuery(String query, Map<String, String> dataSource, String... args) throws ClassNotFoundException, SQLException {
        Connection con = null;
        con = JDBCUtils.getConnection(dataSource);
        PreparedStatement stmt = con.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        for (int i = 0; i < args.length; i++) {
            stmt.setString(i + 1, args[i]);
        }

        logger.info(stmt.toString());
        ResultSet rs = stmt.executeQuery();
        rs.next();
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        List<Map<String, Object>> list = new ArrayList<>();
        int currRowIndex = rs.getRow();
        rs.beforeFirst();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>(columns);
            for (int i = 1; i <= columns; ++i) {
                row.put(meta.getColumnName(i), rs.getObject(i));
            }
            list.add(row);
        }
        rs.absolute(currRowIndex);
        rs.close();
        con.close();
        return list;
    }

    private static void apppendDefaultTableName(StringBuffer sqlBuffer, String script) {
        Matcher createMatcher = CREATE_CLAUSE_PATTERN.matcher(script);
        if (createMatcher.find()) {
            String group = createMatcher.group();
            StringBuilder tableNameBuilder = new StringBuilder();
            int i = group.length();
            while (--i > 0) {
                char c = group.charAt(i);
                if (c > DSLUtils.BLANK_SPACE) {
                    tableNameBuilder.append(c);
                    break;
                }
            }
            while (--i > 0) {
                char c = group.charAt(i);
                if (c > DSLUtils.BLANK_SPACE) {
                    tableNameBuilder.append(c);
                } else {
                    break;
                }
            }
            sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(SQLUtils.wrapString(TABLE_NAME));
            SQLUtils.apppendEquals(sqlBuffer);
            sqlBuffer.append(SQLUtils.wrapString(tableNameBuilder.reverse().toString()));
        }
    }

    private String convertDataType(Map<String, Object> map) {
        String dataType = map.get("DATA_TYPE").toString();
        if ("varchar".equals(dataType)) {
            return dataType + "(" + map.get("COLUMN_SIZE").toString() + ")";
        } else if ("decimal".equals(dataType)) {
            return dataType + "(" + map.get("COLUMN_SIZE") + "," + map.get("DECIMAL_DIGITS") + ")";
        } else if ("datetime".equals(dataType)) {
            return "timestamp(3)";
        } else {
            return dataType;
        }
    }

}
