package net.manmon.dbloader;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

public class CheckTablesExist {
    private static Locale locale = Locale.FRANCE;

    private static Logger logger = Logger.getLogger(CheckTablesExist.class);

    private static DateTimeFormatter dtFormatYearTableName = DateTimeFormatter.ofPattern("yyyy");
    private static DateTimeFormatter dtFormatMonthTableName = DateTimeFormatter.ofPattern("yyyy_MM");
    private static DateTimeFormatter dtFormatWeekTableName = DateTimeFormatter.ofPattern("yyyy_w");
    private static DateTimeFormatter dtFormatDate = DateTimeFormatter.ofPattern("yyyy-MM-ddZ");
    private static DateTimeFormatter dtFormatDayTable = DateTimeFormatter.ofPattern("yyyy_MM_dd");

    private TemporalField fieldISO = WeekFields.of(locale).dayOfWeek();
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ssZ");
    private ZoneId zoneId = ZoneId.of("UTC");

    protected ConcurrentHashMap<String, Boolean> tablesInDb = new ConcurrentHashMap<>();
    private Connection conn;
    private Long dbId;
    private String dbUrl;

    public CheckTablesExist(String dbUrl, Long dbId) throws SQLException {
        this.conn = DriverManager.getConnection(dbUrl);
        this.dbId = dbId;
        this.dbUrl = dbUrl;
        ResultSet rs = conn.createStatement().executeQuery("select table_name from information_schema.tables where table_schema='public'");
        synchronized (tablesInDb) {
            while (rs.next()) {
                tablesInDb.put(rs.getString(1), false);
            }
        }
    }

    private void createIndexes(String tableName) throws SQLException {
        executeSQL("CREATE UNIQUE INDEX IF NOT EXISTS "+tableName+"_uniq_idx ON "+tableName+"(hostid,dataid,key1,key2,dt)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_dt_idx ON "+tableName+"(dt)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_hostid_idx ON "+tableName+"(hostid)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_dataid_idx ON "+tableName+"(dataid)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_key1_idx ON "+tableName+"(key1)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_key2_idx ON "+tableName+"(key2)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_value_desc_index ON "+tableName+"(value desc)");
        executeSQL("CREATE UNIQUE INDEX IF NOT EXISTS "+tableName+"_str_uniq_idx ON "+tableName+"_str(hostid,dataid,key1,key2,dt)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_value_desc_index ON "+tableName+"_str(value desc)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_dt_idx ON "+tableName+"_str(dt)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_hostid_idx ON "+tableName+"_str(hostid)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_dataid_idx ON "+tableName+"_str(dataid)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_key1_idx ON "+tableName+"_str(key1)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_key2_idx ON "+tableName+"_str(key2)");
/*        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_hostid_idx ON "+tableName+"(hostid)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_dataid_idx ON "+tableName+"(dataid)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_key1_idx ON "+tableName+"(key1)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_key2_idx ON "+tableName+"(key2)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_dt_idx ON "+tableName+"(dt)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_value_idx ON "+tableName+"(value)");

        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_hostid_idx ON "+tableName+"_str(hostid)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_dataid_idx ON "+tableName+"_str(dataid)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_key1_idx ON "+tableName+"_str(key1)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_key2_idx ON "+tableName+"_str(key2)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_dt_idx ON "+tableName+"_str(dt)");
        executeSQL("CREATE INDEX IF NOT EXISTS "+tableName+"_str_value_idx ON "+tableName+"_str(value)");
        */

    }

    private void executeSQL(String sql) throws SQLException {
        boolean dbConnActive = false;
        while (!dbConnActive) {
            try {
                conn.createStatement().executeQuery("SELECT 1");
                dbConnActive = true;
            } catch (Exception e) {
                logger.error("Error with SQL connection");
                try {
                    Thread.sleep(5000L);
                    conn = DriverManager.getConnection(dbUrl);
                } catch (Exception ex) {

                }
            }
        }
        conn.createStatement().execute(sql);
        logger.debug("SQL:"+sql);
    }

    private void createParentTable(String tableName) throws SQLException {
        synchronized (tablesInDb) {
            if (!tablesInDb.containsKey(tableName)) {
                executeSQL("CREATE TABLE IF NOT EXISTS " + tableName + " (hostid bigint, dataid bigint, key1 varchar, key2 bigint, dt timestamp with time zone, value bigint) PARTITION BY RANGE (dt)");
                executeSQL("CREATE TABLE IF NOT EXISTS " + tableName + "_str (hostid bigint, dataid bigint, key1 varchar, key2 bigint, dt timestamp with time zone, value varchar(1024)) PARTITION BY RANGE (dt)");
                tablesInDb.put(tableName, false);
            }
        }
    }

    private void createPartitionTable(String parentTableName, String tableName, String start, String end) throws SQLException {
        synchronized (tablesInDb) {
            if (!tablesInDb.containsKey(tableName)) {
                executeSQL("CREATE TABLE IF NOT EXISTS " + tableName + " PARTITION OF " + parentTableName + " FOR VALUES FROM ('" + start + "') TO ('" + end + "');");
                executeSQL("CREATE TABLE IF NOT EXISTS  " + tableName + "_str PARTITION OF " + parentTableName + "_str FOR VALUES FROM ('" + start + "') TO ('" + end + "');");
                createIndexes(tableName);
                tablesInDb.put(tableName, false);
            }
        }
    }

    private void createPlainTable(String tableName) throws SQLException {
        synchronized (tablesInDb) {
            if (!tablesInDb.containsKey(tableName)) {
                executeSQL("CREATE TABLE IF NOT EXISTS " + tableName + " (hostid bigint, dataid bigint, key1 varchar, key2 bigint, dt timestamp with time zone, value bigint)");
                executeSQL("CREATE TABLE IF NOT EXISTS " + tableName + "_str (hostid bigint, dataid bigint, key1 varchar, key2 bigint, dt timestamp with time zone, value varchar(1024))");
                createIndexes(tableName);
                tablesInDb.put(tableName, false);
            }
        }
    }

    private void createWeekIfNotExists(ZonedDateTime zonedDateTime) throws SQLException {
        String parentTableName = "data_week_"+dbId;
        if (!tablesInDb.containsKey(parentTableName)) {
            createParentTable(parentTableName);
        }
        String year = zonedDateTime.with(TemporalAdjusters.firstDayOfYear()).format(dtFormatYearTableName);
        String yearTableName = "data_week_"+dbId+"_"+year;
        if (!tablesInDb.containsKey(yearTableName)) {
            String yearStart = zonedDateTime.with(TemporalAdjusters.firstDayOfYear()).format(dtFormatDate);
            String yearEnd = zonedDateTime.with(TemporalAdjusters.firstDayOfNextYear()).format(dtFormatDate);
            createPartitionTable(parentTableName, yearTableName, yearStart, yearEnd);
        }
    }

    private void createDayIfNotExists(ZonedDateTime zonedDateTime) throws SQLException {
        String parentTableName = "data_day_"+dbId;
        if (!tablesInDb.containsKey(parentTableName)) {
            createParentTable(parentTableName);
        }
        String yearAndMonth = zonedDateTime.with(TemporalAdjusters.firstDayOfMonth()).format(dtFormatMonthTableName);
        String monthTableName = "data_day_"+dbId+"_"+yearAndMonth;
        if (!tablesInDb.containsKey(monthTableName)) {
            String monthStart = zonedDateTime.with(TemporalAdjusters.firstDayOfMonth()).format(dtFormatDate);
            String monthEnd = zonedDateTime.with(TemporalAdjusters.firstDayOfMonth()).plusMonths(1).format(dtFormatDate);
            createPartitionTable(parentTableName, monthTableName, monthStart, monthEnd);
        }
    }

    private void createHourIfNotExists(ZonedDateTime zonedDateTime) throws SQLException {
        String parentTableName = "data_hour_"+dbId;
        if (!tablesInDb.containsKey(parentTableName)) {
            createParentTable(parentTableName);
        }
        String yearAndWeek = zonedDateTime.with(fieldISO, 1).format(dtFormatWeekTableName);
        String weekTableName = "data_hour_"+dbId+"_"+yearAndWeek;
        if (!tablesInDb.containsKey(weekTableName)) {
            String weekStart = zonedDateTime.with(fieldISO, 1).format(dtFormatDate);
            String weekEnd = zonedDateTime.plusWeeks(1).with(fieldISO, 1).format(dtFormatDate);
            createPartitionTable(parentTableName, weekTableName, weekStart, weekEnd);
        }
    }

    private void createMinuteIfNotExists(ZonedDateTime zonedDateTime, String dataType) throws SQLException {
        String parentTableName = "data_"+dataType+"_"+dbId;
        if (!tablesInDb.containsKey(parentTableName)) {
            createParentTable(parentTableName);
        }
        String yearMonthAndDay = zonedDateTime.format(dtFormatDayTable);
        String dayTable = "data_"+dataType+"_"+dbId+"_"+yearMonthAndDay;
        if (!tablesInDb.containsKey(dayTable)) {
            String dayStart = zonedDateTime.format(dtFormatDate);
            String dayEnd = zonedDateTime.plusDays(1).format(dtFormatDate);
            createPartitionTable(parentTableName, dayTable, dayStart, dayEnd);
        }
    }

    protected void checkWithDt(ZonedDateTime zonedDateTime, String topic) throws ParseException, SQLException {
    //    Instant instant = sdf.parse(dt).toInstant();
    //    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        if (topic.equals("data_min_"+dbId)) {
            createMinuteIfNotExists(zonedDateTime, "min");
        } else if (topic.equals("data_5min_"+dbId)) {
            createMinuteIfNotExists(zonedDateTime, "5min");
        } else if (topic.equals("data_15min_"+dbId)) {
            createMinuteIfNotExists(zonedDateTime, "15min");
        } else if (topic.equals("data_hour_"+dbId)) {
            createHourIfNotExists(zonedDateTime);
        } else if (topic.equals("data_day_"+dbId)) {
            createDayIfNotExists(zonedDateTime);
        } else if (topic.equals("data_week_"+dbId)) {
            createWeekIfNotExists(zonedDateTime);
        } else if (topic.equals("data_month_"+dbId)) {
            if (!tablesInDb.containsKey("data_month_"+dbId)) {
                createPlainTable("data_month_"+dbId);
            }
        } else if (topic.equals("data_year_"+dbId)) {
            if (!tablesInDb.containsKey("data_year_"+dbId)) {
                createPlainTable("data_year_"+dbId);
            }
        } else {
            logger.error("Topic "+topic+" is invalid!");
        }
    }


}
