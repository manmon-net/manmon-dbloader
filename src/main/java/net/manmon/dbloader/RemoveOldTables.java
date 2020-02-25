package net.manmon.dbloader;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

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
import java.util.concurrent.ConcurrentHashMap;

public class RemoveOldTables {
    private Connection conn;
    private static Logger logger;

    public static void main(String[] args) throws Exception {
        RemoveOldTables removeOldTables = new RemoveOldTables();
        PropertyConfigurator.configure(removeOldTables.getClass().getClassLoader().getResourceAsStream("remover-log4j.properties"));
        logger = Logger.getLogger(RemoveOldTables.class);
        removeOldTables.run();
    }

    public RemoveOldTables() {
    }

    public void run() {
        String dbUrl = "jdbc:postgresql://127.0.0.1/manmon_data?user=manmon_data&password=kanjkvajnkivaWERjkvsa43Anavmkxa";
        try {
            conn = DriverManager.getConnection(dbUrl);
            checkTables();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void removeOldTable(String tableName) throws SQLException {
        String sql = "DROP TABLE "+tableName;
        logger.info(sql);
        conn.createStatement().execute(sql);
    }

    private boolean minuteDataTableIsOld(String tableName, Long days) {
        if (tableName.endsWith("_str")) {
            tableName = tableName.replaceFirst("_str$","")+" 00:00+0000";
        } else {
            tableName = tableName+" 00:00+0000";
        }

        ZonedDateTime tableDt = ZonedDateTime.parse(tableName, DateTimeFormatter.ofPattern("yyyy_MM_dd HH:mmZ"));
        return ZonedDateTime.now().minusDays(days).isAfter(tableDt);
    }

    private boolean hourDataTableIsOld(String tableName, Long days) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy_w");
        Instant i = simpleDateFormat.parse(tableName).toInstant();
        ZonedDateTime tableDt = ZonedDateTime.ofInstant(i, ZoneId.of("UTC"));
        return ZonedDateTime.now().minusDays(days).isAfter(tableDt);
    }

    private boolean dayDataTableIsOld(String tableName, Long days) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy_MM");
        Instant monthInstant = simpleDateFormat.parse(tableName).toInstant();
        ZonedDateTime tableDt = ZonedDateTime.ofInstant(monthInstant, ZoneId.of("UTC"));
        return ZonedDateTime.now().minusDays(days).isAfter(tableDt);
    }

    private boolean weekDataTableIsOld(String tableName, Long days) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy");
        Instant monthInstant = simpleDateFormat.parse(tableName).toInstant();
        ZonedDateTime tableDt = ZonedDateTime.ofInstant(monthInstant, ZoneId.of("UTC"));
        return ZonedDateTime.now().minusDays(days).isAfter(tableDt);
    }

    public void checkTables() throws Exception {
        long dbId = new Long(System.getenv("DBID")).longValue();

        ConcurrentHashMap<String, Long> topicInfos = new ConcurrentHashMap<>();
        for (String topicInfo : System.getenv("TOPICINFO").split(",")) {
            String[] s = topicInfo.split(":");
            topicInfos.put(s[0]+"_"+dbId, Long.valueOf(s[3]));
        }

        ConcurrentHashMap<String,Boolean> tablesInDb = new ConcurrentHashMap<>();
        ResultSet rs = conn.createStatement().executeQuery("select table_name from information_schema.tables where table_schema='public'");
        while (rs.next()) {
            tablesInDb.put(rs.getString(1), false);
        }

        for (String dbTableName : tablesInDb.keySet()) {
            for (String tableName : topicInfos.keySet()) {
                if (dbTableName.startsWith(tableName) && !dbTableName.equals(tableName) && !dbTableName.equals(tableName+"_str")) {
                    String dtTableName = dbTableName.replaceFirst("^"+tableName+"_","");
                    if (tableName.equals("data_min_"+dbId) || tableName.equals("data_5min_"+dbId) || tableName.equals("data_15min_"+dbId)) {
                        if (minuteDataTableIsOld(dtTableName, topicInfos.get(tableName))) {
                            removeOldTable(dbTableName);
                        }
                    } else if (tableName.equals("data_hour_"+dbId)) {
                        if (hourDataTableIsOld(dtTableName, topicInfos.get(tableName))) {
                            removeOldTable(dbTableName);
                        }
                    } else if (tableName.equals("data_day_"+dbId)) {
                        if (dayDataTableIsOld(dtTableName, topicInfos.get(tableName))) {
                            removeOldTable(dbTableName);
                        }
                    } else if (tableName.equals("data_week_"+dbId)) {
                        if (weekDataTableIsOld(dtTableName, topicInfos.get(tableName))) {
                            removeOldTable(dbTableName);
                        }
                    } else if (tableName.equals("data_month_"+dbId)) {
                    } else if (tableName.equals("data_year_"+dbId)) {
                    } else {
                        System.err.println("Invalid table name: "+tableName);
                    }
                }
            }
        }
    }
}
