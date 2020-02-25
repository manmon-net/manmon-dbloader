package net.manmon.dbloader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;

import kafka.admin.RackAwareMode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.flywaydb.core.Flyway;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import org.postgresql.util.PSQLException;

public class Loader {
	public static ConcurrentLinkedQueue<ManmonData> q = new ConcurrentLinkedQueue<>();
	public static List<ManmonPartition> commitPartitions = new ArrayList<>();
	
	private KafkaConsumer<Long, byte[]> consumer = null;
	private ExecutorService extractExecutor;
	private ExecutorService dbExecutor;
	private long dbCheckInterval;
	private static AtomicBoolean shutdown = new AtomicBoolean(false);

	private static Logger logger = null;

	protected CheckTablesExist checkTablesExist;

	public static void main(String[] args) throws Exception {
        Flyway flyway = Flyway.configure().dataSource("jdbc:postgresql://manmon-data-db/manmon_data","manmon_data","kanjkvajnkivaWERjkvsa43Anavmkxa").load();
		flyway.migrate();
		Loader loader = new Loader();
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

				while (!q.isEmpty() || !commitPartitions.isEmpty()) {
					try {
						Thread.sleep(100L);
					} catch (InterruptedException e) {
					    System.err.println("Interrupted exception");
					}
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
	
	private class ManmonPartition {
		private ManmonPartition(TopicPartition topicPartition, Long offset) {
			this.topicPartition = topicPartition;
			this.offset = offset;
		}
		
		private TopicPartition topicPartition;
		private Long offset;
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
		adminZkClient.createTopic(topic,partitions,replication,topicConfig,RackAwareMode.Enforced$.MODULE$);
        zkClient.close();
	}

	private void getKafkaConsumer() {

    }

	private void start() throws Exception {
		List<String> topics = new ArrayList<>();						
		
		long dbId = new Long(System.getenv("DBID")).longValue();
		
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
	    consumer = new KafkaConsumer<>(kafkaProps);
	    Map<String, List<PartitionInfo>> kafkaTopics = consumer.listTopics();
	    
	    Map<String, String> topicInfos = new HashMap<>();
	    for (String topicInfo : System.getenv("TOPICINFO").split(",")) {
	    	String[] s = topicInfo.split(":");
	    	topicInfos.put(s[0]+"_"+dbId, topicInfo);
	    }
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
	    
		String dbUrl = "jdbc:postgresql://manmon-data-db/manmon_data?user=manmon_data&password=kanjkvajnkivaWERjkvsa43Anavmkxa";
		checkTablesExist = new CheckTablesExist(dbUrl, dbId);
		//new DbTableCreator().checkTables(DriverManager.getConnection(dbUrl), dbId);

	    extractExecutor = Executors.newFixedThreadPool(new Integer(System.getenv("EXTRACT_THREADS")).intValue());
	    int dbExecutorThreads = new Integer(System.getenv("DB_THREADS")).intValue();
	    dbExecutor = Executors.newFixedThreadPool(dbExecutorThreads);	    	  
	    
	    HashSet<Future<Boolean>> dbFutures = new HashSet<>();
	    for (int i=0; i<dbExecutorThreads; i++) {
	    	dbFutures.add(dbExecutor.submit(new DbLoader(dbUrl)));		
	    }
	    		
		boolean running = true;
		while (running) { 
			try {
			    try {
                    pollKafka(topics);
                } catch (Exception ex) {
			        logger.error("Error with kafka",ex);
			        Thread.sleep(5000L);
                }
				HashSet<Future<Boolean>> dbFuturesToRemove = new HashSet<>();		
				for (Future<Boolean> dbFuture : dbFutures) {
					if (dbFuture.isDone()) {
						dbFuturesToRemove.add(dbFuture);
					}
				}
				for (Future<Boolean> dbFuture : dbFuturesToRemove) {
					dbFutures.remove(dbFuture);
					dbFutures.add(dbExecutor.submit(new DbLoader(dbUrl)));			
				}
			} catch (Exception e) {
				logger.error("Exception which should not occur", e);
			}
		}
	}
	

	
	private class DbLoader implements Callable<Boolean> {
		private Connection conn;
		private CopyManager copyManager;
		private String url;
		
		private DbLoader(String url) {
			this.url = url;
		}
		
		@Override
		public Boolean call() throws Exception {
            conn = DriverManager.getConnection(url);
            try {
	            conn.setAutoCommit(false);
	            copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
	            boolean running = true;
	            long lastDbCheck = new Date().getTime();
	            while (running) {
	            	try {
						ManmonData manmonData = q.poll();
						if (manmonData != null) {
							copyManager.copyIn("COPY " + manmonData.getTopic() + " FROM STDIN WITH DELIMITER '#'", manmonData.getBis());
							copyManager.copyIn("COPY " + manmonData.getTopic() + "_str FROM STDIN WITH DELIMITER '#'", manmonData.getStrBis());

							lastDbCheck = new Date().getTime();
							TopicPartition partition = new TopicPartition(manmonData.getTopic(), manmonData.getPartition());
							logger.info("Adding commit partition "+manmonData.getTopic()+" "+(manmonData.getOffset()+1));
							synchronized (Loader.commitPartitions) {
                                Loader.commitPartitions.add(new ManmonPartition(partition, manmonData.getOffset()));
                            }
                            //consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(manmonData.getOffset() + 1)));
                            conn.commit();
                        } else {

							Thread.sleep(10L);
							if (lastDbCheck < (new Date().getTime() - (dbCheckInterval * 1000L))) {
								dbCheckInterval = new Date().getTime();
								conn.createStatement().executeQuery("SELECT 1");
							}
						}
					} catch (PSQLException ex) {
						logger.error("Thread:"+Thread.currentThread().getName()+" - Error in DB connection. Trying to reconnect.", ex);
						try {
							conn.close();
							conn = null;
							copyManager = null;
						} catch (Exception ee) {

						}
						boolean notConnected = true;
						while (notConnected) {
							try {
								Thread.sleep(5000L);
								conn = DriverManager.getConnection(url);
								conn.setAutoCommit(false);
								copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
								lastDbCheck = new Date().getTime();
								conn.createStatement().executeQuery("SELECT 1");
								notConnected = false;
							} catch (PSQLException ee) {
								logger.error("Thread:"+Thread.currentThread().getName()+" - Error in DB connection. Trying to reconnect.", ex);
							}
						}
					}
	            }
            } catch (Exception e) {
                logger.error("Exception at db load", e);
            } finally {
				conn.close();
			}
			return null;
		}
		
	}
	
	private void pollKafka(List<String> topics) throws Exception {
		long pollFrequency = new Long(System.getenv("KAFKA_POLL_FREQUENCY")).longValue();
		synchronized (Loader.commitPartitions) {

            while (Loader.commitPartitions.size()>0) {
                ManmonPartition mmPart = Loader.commitPartitions.remove(0);
                //ManmonPartition mmPart = Loader.commitPartitions.poll();
                logger.info("Commiting " + mmPart.topicPartition.topic() + " " + (mmPart.offset + 1));
                consumer.commitSync(Collections.singletonMap(mmPart.topicPartition, new OffsetAndMetadata(mmPart.offset + 1)));
            }
        }
		while (!shutdown.get()) {
			ConsumerRecords<Long, byte[]> kafkaRecords = consumer.poll(Duration.ofMillis(pollFrequency));
			if (!kafkaRecords.isEmpty()) {			
				for (String topic : topics) {				
					processKafkaRecords(topic, kafkaRecords.records(topic));
				}
			}
		}
	}
	
	private class Extractor implements Callable<FutureWithPartitionAndData> {
		private byte[] compressedData;
		private int partition;
		private long hostId;
		private String topic;

		public Extractor(int partition, byte[] compressedData, long hostId, String topic) {
			this.compressedData = compressedData;
			this.partition = partition;
			this.hostId = hostId;
			this.topic = topic;
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

		@Override
		public FutureWithPartitionAndData call() throws Exception {			
			GZIPInputStream gzIn = new GZIPInputStream(new ByteArrayInputStream(compressedData));			
			BufferedReader br = new BufferedReader(new InputStreamReader(gzIn));
			ByteArrayOutputStream dataBos = new ByteArrayOutputStream();
			ByteArrayOutputStream strDataBos = new ByteArrayOutputStream();

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
						String key2 = "\\N";
						if (pieces[2].length()>0) {
							key2 = new Long(pieces[2]).toString();	
						}		
						
						if (stringValues) {
							String value = pieces[3];
							strDataBos.write((hostId+"#"+dataId+"#"+key1+"#"+key2+"#"+dt+"#"+value+"\n").getBytes());
						} else {
							long value = new Long(pieces[3]);					
							dataBos.write((hostId+"#"+dataId+"#"+key1+"#"+key2+"#"+dt+"#"+value+"\n").getBytes());
						}
					} 
					line = br.readLine();
				}
			} catch (Exception e) {
			    logger.error("Exception parsing data from kafka",e);
				return new FutureWithPartitionAndData(partition, new ByteArrayOutputStream().toByteArray(), new ByteArrayOutputStream().toByteArray());
			}
			br.close();			
			return new FutureWithPartitionAndData(partition, dataBos.toByteArray(), strDataBos.toByteArray());
		}
		
	}
	
	private class FutureWithPartitionAndData {
		private int partition;
		private byte[] data;
		private byte[] strData;
		
		public FutureWithPartitionAndData(int partition, byte[] data, byte[] strData) {
			this.partition = partition;
			this.data = data;
			this.strData = strData;
		}
		
 		public int getPartition() {
			return partition;
		}
				
		public byte[] getData() {
			return data;
		}		
		
		public byte[] getStrData() {
			return strData;
		}
	}
	
	private void processKafkaRecords(String topic, Iterable<ConsumerRecord<Long, byte[]>> topicRecords) throws Exception {			
		HashMap<Integer, ByteArrayOutputStream> data = new HashMap<>();
		HashMap<Integer, ByteArrayOutputStream> strData = new HashMap<>();

		HashMap<Integer, Long> offsets = new HashMap<>();
		HashSet<Future<FutureWithPartitionAndData>> futures = new HashSet<>();
		for (ConsumerRecord<Long, byte[]> record : topicRecords) {
			futures.add(extractExecutor.submit(new Extractor(record.partition(), record.value(), record.key(), topic)));
			if (offsets.containsKey(record.partition())) {
				if (record.offset() > offsets.get(record.partition())) {
					offsets.put(record.partition(), record.offset());
				}
			} else {
				offsets.put(record.partition(), record.offset());
			}
		}

		while (futures.size() > 0) {
			HashSet<Future<FutureWithPartitionAndData>> futuresToRemove = new HashSet<>();
			for (Future<FutureWithPartitionAndData> future : futures) {
				if (future.isDone()) {
					futuresToRemove.add(future);
					FutureWithPartitionAndData partitionAndData = future.get();
					if (!data.containsKey(partitionAndData.getPartition())) {
						data.put(partitionAndData.getPartition(), new ByteArrayOutputStream());				
					}
					data.get(partitionAndData.getPartition()).write(partitionAndData.getData());
					if (!strData.containsKey(partitionAndData.getPartition())) {
						strData.put(partitionAndData.getPartition(), new ByteArrayOutputStream());				
					}
					strData.get(partitionAndData.getPartition()).write(partitionAndData.getStrData());					
				}
			}
			for (Future<FutureWithPartitionAndData> future : futuresToRemove) {
				futures.remove(future);
			}
		}
		for (Integer partitionId : data.keySet()) {
			q.add(new ManmonData(topic, partitionId, offsets.get(partitionId) , new ByteArrayInputStream(data.get(partitionId).toByteArray()), new ByteArrayInputStream(strData.get(partitionId).toByteArray())));
		}
	}
}
