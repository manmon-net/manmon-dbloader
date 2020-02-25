package net.manmon.dbloader;

import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Time;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.flywaydb.core.Flyway;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PSQLException;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

public class NewLoader {
    private static Logger logger = null;
    private static AtomicBoolean shutdown = new AtomicBoolean(false);
    private long dbCheckInterval;
    private KafkaConsumer<Long, byte[]> consumer = null;
    private Long pollFrequency = null;
    private Long dbId = null;

    private CheckTablesExist checkTablesExist = null;

    private HashSet addedDataTypes = new HashSet();
    private StringWriter dataMinPsw = null;
    private StringWriter dataMinStrPsw = null;
    private StringWriter data5MinPsw = null;
    private StringWriter data5MinStrPsw = null;
    private StringWriter data15MinPsw = null;
    private StringWriter data15MinStrPsw = null;
    private StringWriter dataHourPsw = null;
    private StringWriter dataHourStrPsw = null;
    private StringWriter dataDayPsw = null;
    private StringWriter dataDayStrPsw = null;
    private StringWriter dataWeekPsw = null;
    private StringWriter dataWeekStrPsw = null;
    private StringWriter dataMonthPsw = null;
    private StringWriter dataMonthStrPsw = null;

    private static String dbUrl = "jdbc:postgresql://manmon-data-db/manmon_data";
    private static String dbUser = "manmon_data";
    private static String dbPwd = "kanjkvajnkivaWERjkvsa43Anavmkxa";
    public static void main(String[] args) throws Exception {
        Flyway flyway = Flyway.configure().dataSource(dbUrl, dbUser, dbPwd).load();
        flyway.migrate();
        NewLoader loader = new NewLoader();
        PropertyConfigurator.configure(loader.getClass().getClassLoader().getResourceAsStream("log4j.properties"));
        logger = Logger.getLogger(Loader.class);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Shutting down");
                logger.info("Shutting down");
                shutdown.set(true);

                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    System.err.println("Interrupted exception");
                }
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    System.err.println("Interrupted exception");
                }
                System.out.println("Exited");
                logger.info("Exited");
            }
        });
        loader.start();
    }


    protected void start() throws Exception {
        pollFrequency = new Long(System.getenv("KAFKA_POLL_FREQUENCY")).longValue();
        List<String> topics = new ArrayList<>();
        dbId = new Long(System.getenv("DBID")).longValue();

        for (String topic : System.getenv("KAFKA_TOPICS").split(",")) {
            topics.add(topic+"_"+dbId);
        }

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", System.getenv("KAFKA_SERVERS"));
        kafkaProps.put("group.id", System.getenv("KAFKA_GROUPID"));
        kafkaProps.put("max.poll.records", System.getenv("KAFKA_MAX_POLL_RECORDS"));
        kafkaProps.put("enable.auto.commit", "false");
        kafkaProps.put("auto.offset.reset", "earliest");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        Map<String, String> topicInfos = new HashMap<>();
        for (String topicInfo : System.getenv("TOPICINFO").split(",")) {
            String[] s = topicInfo.split(":");
            topicInfos.put(s[0]+"_"+dbId, topicInfo);
        }

        consumer = new KafkaConsumer<>(kafkaProps);
        Map<String, List<PartitionInfo>> kafkaTopics = consumer.listTopics();
        for (String topic : topics) {
            if (!kafkaTopics.containsKey(topic)) {
                String[] s = topicInfos.get(topic).split(":");
                int partitions = new Integer(s[1]).intValue();
                int replication = new Integer(s[2]).intValue();;
                long daysRetention = new Long(s[3]).longValue();;
                createKafkaTopic(topic, partitions, replication, daysRetention);
            }
        }
        consumer.subscribe(topics);

        dbCheckInterval = new Long(System.getenv("DB_CHECK_INTERVAL")).longValue();
        checkTablesExist = new CheckTablesExist(dbUrl+"?user="+dbUser+"&password="+dbPwd, dbId);

        boolean running = true;
        while (running) {
            try {
                try {
                    pollKafka(topics);
                } catch (Exception ex) {
                    consumer.close();
                    logger.error("Error loading data",ex);
                    Thread.sleep(5000L);
                    consumer = new KafkaConsumer<>(kafkaProps);
                    consumer.subscribe(topics);
                }
            } catch (Exception e) {
                logger.error("Exception which should not occur", e);
            }
        }
    }

    private void createKafkaTopic(String topic, int partitions, int replication, long daysRetention) throws Exception {
        String zookeeperConnect = System.getenv("ZOOKEEPER_SERVERS");
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        int maxInFlightRequests = 10;
        Time time = Time.SYSTEM;
        String metricGroup = "myGroup";
        String metricType = "myType";
        boolean isSecureKafkaCluster = false;

        KafkaZkClient zkClient = KafkaZkClient.apply(zookeeperConnect,isSecureKafkaCluster,sessionTimeoutMs,
                connectionTimeoutMs,maxInFlightRequests,time,metricGroup,metricType);
        AdminZkClient adminZkClient = new AdminZkClient(zkClient);


        Properties topicConfig = new Properties();
        long msRetention = daysRetention*1000L*60L*60L*24L;
        if (msRetention<0) {
            throw new Exception("Negative retention topic="+topic+" retentionMs="+msRetention+" daysRetention="+daysRetention);
        }
        topicConfig.setProperty("retention.ms", ""+msRetention);
        adminZkClient.createTopic(topic,partitions,replication,topicConfig, RackAwareMode.Enforced$.MODULE$);
        zkClient.close();
    }

    private void pollKafka(List<String> topics) throws Exception {
        while (!shutdown.get()) {
            ConsumerRecords<Long, byte[]> kafkaRecords = consumer.poll(Duration.ofMillis(pollFrequency));
            if (!kafkaRecords.isEmpty()) {
                loadData(kafkaRecords, topics);
            }
        }
    }
    private void loadData(ConsumerRecords<Long, byte[]> kafkaRecords, List<String> topics) throws Exception {
        resetPsws();
        for (String topic : topics) {
            for (ConsumerRecord<Long, byte[]> record : kafkaRecords.records(topic)) {
                try {
                    extractor(record.value(), record.key(), topic);
                } catch (Exception e) {
                    logger.error("Exception at extacting data",e);
                }
            }
        }
        persistToDb();
        consumer.commitAsync();
    }

    private void copyData(String tableName, Connection conn, CopyManager copyManager, StringWriter sw, boolean stringValue, boolean hadException) throws Exception {
        if (!hadException) {
            copyManager.copyIn("COPY " + tableName + "(hostid,dataid,key1,key2,dt,value) FROM STDIN WITH delimiter '#'", new StringReader(sw.toString()));
        } else {
            if (!stringValue) {
                conn.createStatement().execute("CREATE TEMP TABLE tmp_" + tableName + "(hostid BIGINT,dataid BIGINT,key1 VARCHAR(512),key2 BIGINT,dt TIMESTAMP WITH TIME ZONE,value BIGINT) ON COMMIT DROP");
            } else {
                conn.createStatement().execute("CREATE TEMP TABLE tmp_" + tableName + "(hostid BIGINT,dataid BIGINT,key1 VARCHAR(512),key2 BIGINT,dt TIMESTAMP WITH TIME ZONE,value VARCHAR(255)) ON COMMIT DROP");
            }
            copyManager.copyIn("COPY tmp_" + tableName + "(hostid,dataid,key1,key2,dt,value) FROM STDIN WITH delimiter '#'", new StringReader(sw.toString()));
            conn.createStatement().execute("INSERT INTO " + tableName + " SELECT * FROM tmp_" + tableName + " ON CONFLICT DO NOTHING");
        }
    }

    private void parseForCopy(Connection conn, CopyManager copyManager, boolean hadException) throws Exception {
        if (addedDataTypes.contains("data_min")) {
            copyData("data_min_"+dbId, conn, copyManager, dataMinPsw, false, hadException);
        }
        if (addedDataTypes.contains("data_min_str")) {
            copyData("data_min_"+dbId+"_str", conn, copyManager, dataMinStrPsw, true, hadException);
        }
        if (addedDataTypes.contains("data_5min")) {
            copyData("data_5min_"+dbId, conn, copyManager, data5MinPsw, false, hadException);
        }
        if (addedDataTypes.contains("data_5min_str")) {
            copyData("data_5min_"+dbId+"_str", conn, copyManager, data5MinStrPsw, true, hadException);
        }
        if (addedDataTypes.contains("data_15min")) {
            copyData("data_15min_"+dbId, conn, copyManager, data15MinPsw, false, hadException);
        }
        if (addedDataTypes.contains("data_15min_str")) {
            copyData("data_15min_"+dbId+"_str", conn, copyManager, data15MinStrPsw, true, hadException);
        }
        if (addedDataTypes.contains("data_hour")) {
            copyData("data_hour_"+dbId, conn, copyManager, dataHourPsw, false, hadException);
        }
        if (addedDataTypes.contains("data_hour_str")) {
            copyData("data_hour_"+dbId+"_str", conn, copyManager, dataHourStrPsw, true, hadException);
        }
        if (addedDataTypes.contains("data_day")) {
            copyData("data_day_"+dbId, conn, copyManager, dataDayPsw, false, hadException);
        }
        if (addedDataTypes.contains("data_day_str")) {
            copyData("data_day_"+dbId+"_str", conn, copyManager, dataDayStrPsw, true, hadException);
        }
        if (addedDataTypes.contains("data_week")) {
            copyData("data_week_"+dbId, conn, copyManager, dataWeekPsw, false, hadException);
        }
        if (addedDataTypes.contains("data_week_str")) {
            copyData("data_week_"+dbId+"_str", conn, copyManager, dataWeekStrPsw, true, hadException);
        }
        if (addedDataTypes.contains("data_month")) {
            copyData("data_month_"+dbId, conn, copyManager, dataMonthPsw, false, hadException);
        }
        if (addedDataTypes.contains("data_month_str")) {
            copyData("data_month_"+dbId+"_str", conn, copyManager, dataMonthStrPsw, true, hadException);
        }
    }

    private void persistToDb() throws Exception {
        Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPwd);
        conn.setAutoCommit(false);
        CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
        try {
            try {
                parseForCopy(conn, copyManager, false);
            } catch (PSQLException pe) {
                if (pe.getMessage().startsWith("ERROR: duplicate key value violates unique constraint")) {
                    conn.rollback();
                    logger.info("Some of the data already exists");
                    parseForCopy(conn, copyManager, true);
                } else {
                    throw new Exception(pe);
                }
            }
            conn.commit();
        } finally {
            conn.close();
        }
    }

    private void extractor(byte[] compressedData, Long hostId, String topic) throws Exception {
        GZIPInputStream gzIn = new GZIPInputStream(new ByteArrayInputStream(compressedData));
        BufferedReader br = new BufferedReader(new InputStreamReader(gzIn));

        try {
            String line = br.readLine();
            String dt = null;
            boolean stringValues = false;
            while (line != null) {
                String[] pieces = line.split("(?<!\\\\)#");
                if (line.startsWith("###DT")) {
                    ZonedDateTime zdt = checkDtIsValid(line);
                    checkTablesExist.checkWithDt(zdt, topic);
                    dt = getDataDateString(zdt.toInstant().toEpochMilli());
                } else if (line.startsWith("###STR")) {
                    stringValues = true;
                } else if (dt != null && pieces.length == 4) {
                    long dataId = new Long(pieces[0]);
                    String key1 = pieces[1];
                    String key2 = "-1";
                    if (pieces[2].length()>0) {
                        key2 = new Long(pieces[2]).toString();
                    }

                    if (stringValues) {
                        String value = pieces[3];
                        String s = hostId+"#"+dataId+"#"+key1+"#"+key2+"#"+dt+"#"+value+"\n";
                        if (topic.equals("data_min_"+dbId)) {
                            dataMinStrPsw.write(s);
                            addedDataTypes.add("data_min_str");
                        } else if (topic.equals("data_5min_"+dbId)) {
                            data5MinStrPsw.write(s);
                            addedDataTypes.add("data_5min_str");
                        } else if (topic.equals("data_15min_"+dbId)) {
                            data15MinStrPsw.write(s);
                            addedDataTypes.add("data_15min_str");
                        } else if (topic.equals("data_hour_"+dbId)) {
                            dataHourStrPsw.write(s);
                            addedDataTypes.add("data_hour_str");
                        } else if (topic.equals("data_day_"+dbId)) {
                            dataDayStrPsw.write(s);
                            addedDataTypes.add("data_day_str");
                        } else if (topic.equals("data_week_"+dbId)) {
                            dataWeekStrPsw.write(s);
                            addedDataTypes.add("data_week_str");
                        } else if (topic.equals("data_month_"+dbId)) {
                            dataMonthStrPsw.write(s);
                            addedDataTypes.add("data_month_str");
                        } else {
                            logger.error("No match for topic "+topic);
                        }
                    } else {
                        long value = new Long(pieces[3]);
                        String s = hostId+"#"+dataId+"#"+key1+"#"+key2+"#"+dt+"#"+value+"\n";
                        if (topic.equals("data_min_"+dbId)) {
                            dataMinPsw.write(s);
                            addedDataTypes.add("data_min");
                        } else if (topic.equals("data_5min_"+dbId)) {
                            data5MinPsw.write(s);
                            addedDataTypes.add("data_5min");
                        } else if (topic.equals("data_15min_"+dbId)) {
                            data15MinPsw.write(s);
                            addedDataTypes.add("data_15min");
                        } else if (topic.equals("data_hour_"+dbId)) {
                            dataHourPsw.write(s);
                            addedDataTypes.add("data_hour");
                        } else if (topic.equals("data_day_"+dbId)) {
                            dataDayPsw.write(s);
                            addedDataTypes.add("data_day");
                        } else if (topic.equals("data_week_"+dbId)) {
                            dataWeekPsw.write(s);
                            addedDataTypes.add("data_week");
                        } else if (topic.equals("data_month_"+dbId)) {
                            dataMonthPsw.write(s);
                            addedDataTypes.add("data_month");
                        } else {
                            logger.error("No match for topic "+topic);
                        }
                    }
                }
                line = br.readLine();
            }
        } catch (Exception e) {
            logger.error("Exception parsing data from kafka",e);
        }
        br.close();
    }


    private ZonedDateTime checkDtIsValid(String line) throws Exception {
        line = line.replaceFirst("###DT#","");
        LocalDateTime ldt = LocalDateTime.parse(line, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime ldtNow = LocalDateTime.now();
        Long minimumDataDateInMilliseconds = ZonedDateTime.of(ldtNow, ZoneId.of("UTC")).minusWeeks(1L).toInstant().toEpochMilli();
        Long maximumDataDateInMilliseconds = ZonedDateTime.of(ldtNow, ZoneId.of("UTC")).plusMinutes(5L).toInstant().toEpochMilli();

        Long dataDateInMilliseconds = ZonedDateTime.of(ldt, ZoneId.of("UTC")).toInstant().toEpochMilli();

        if (dataDateInMilliseconds < minimumDataDateInMilliseconds ) {
            throw new Exception("Too old data - data can be maximum 1 week old");
        } else if (dataDateInMilliseconds > maximumDataDateInMilliseconds) {
            throw new Exception("Too new data - data can be maximum 5 minutes newer");
        } else {
            return ZonedDateTime.of(ldt, ZoneId.of("UTC"));
        }
    }

    private String getDataDateString(Long dataDateInMilliseconds) {
        Date dt = new Date(dataDateInMilliseconds);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
        return sdf.format(dt);
    }

    private void resetPsws() {
        dataMinPsw = new StringWriter();
        dataMinStrPsw = new StringWriter();
        data5MinPsw = new StringWriter();
        data5MinStrPsw = new StringWriter();
        data15MinPsw = new StringWriter();
        data15MinStrPsw = new StringWriter();
        dataHourPsw = new StringWriter();
        dataHourStrPsw = new StringWriter();
        dataDayPsw = new StringWriter();
        dataDayStrPsw = new StringWriter();
        dataWeekPsw = new StringWriter();
        dataWeekStrPsw = new StringWriter();
        dataMonthPsw = new StringWriter();
        dataMonthStrPsw = new StringWriter();
        addedDataTypes = new HashSet<>();
    }
}
