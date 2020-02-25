package net.manmon.dbloader;

import org.apache.log4j.PropertyConfigurator;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;

public class Test {
    private static final Logger logger = LoggerFactory.getLogger(Test.class);


    private Long dbId=1L;
    private static String dbUrl = "jdbc:postgresql://manmon-data-db/manmon_data";
    private static String dbUser = "manmon_data";
    private static String dbPwd = "kanjkvajnkivaWERjkvsa43Anavmkxa";
    public static void main(String[] args) throws Exception{



        Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPwd);
        conn.setAutoCommit(false);
        CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
        StringWriter sw = new StringWriter();
        sw.write("2#20100#manmon-kafka#7730#2019-06-02 12:02:00+00#156");
        Test test = new Test();
        PropertyConfigurator.configure(test.getClass().getClassLoader().getResourceAsStream("log4j.properties"));
        test.copyData("data_min", conn, copyManager, sw,false);
    }
    private void copyData(String tableName, Connection conn, CopyManager copyManager, StringWriter sw, boolean stringValue) throws Exception {
        if (true) {
            try {
                copyManager.copyIn("COPY " + tableName + "_" + dbId + "(hostid,dataid,key1,key2,dt,value) FROM STDIN WITH delimiter '#'", new StringReader(sw.toString()));
            } catch (PSQLException e) {
                if (e.getMessage().startsWith("ERROR: duplicate key value violates unique constraint")) {
                    System.err.println("TRUE");
                }
                System.out.println("XX:"+e.getMessage());
                logger.error("Error adding data", e);
                try {
                    conn.rollback();
                    conn.createStatement().execute("CREATE TEMP TABLE tmp_" + tableName + "_" + dbId + "(hostid BIGINT,dataid BIGINT,key1 VARCHAR(512),key2 BIGINT,dt TIMESTAMP WITH TIME ZONE,value BIGINT)"); // ON COMMIT DROP
                    copyManager.copyIn("COPY tmp_" + tableName + "_" + dbId + "(hostid,dataid,key1,key2,dt, value) FROM STDIN WITH delimiter '#'", new StringReader(sw.toString()));
                    conn.createStatement().execute("INSERT INTO " + tableName + "_" + dbId + " SELECT * FROM tmp_" + tableName + "_" + dbId + " ON CONFLICT DO NOTHING");
                } catch (Exception ex) {
                    logger.error("Error trying to insert data", ex);
                }
            }
        }
    }

}
