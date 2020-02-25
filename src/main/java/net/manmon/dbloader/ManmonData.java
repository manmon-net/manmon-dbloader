package net.manmon.dbloader;

import java.io.ByteArrayInputStream;
import java.io.Serializable;

public class ManmonData implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String topic;
	private int partition;
	private long offset;
	private ByteArrayInputStream bis;
	private ByteArrayInputStream strBis;
	
	public ManmonData(String topic, int partition, long offset, ByteArrayInputStream bis, ByteArrayInputStream strBis) {
		this.topic = topic;
		this.partition = partition;
		this.offset = offset;
		this.bis = bis;
		this.strBis = strBis;
	}
	
	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public ByteArrayInputStream getBis() {
		return bis;
	}

	public void setBis(ByteArrayInputStream bis) {
		this.bis = bis;
	}

	public ByteArrayInputStream getStrBis() {
		return strBis;
	}

	public void setStrBis(ByteArrayInputStream strBis) {
		this.strBis = strBis;
	}	

}
