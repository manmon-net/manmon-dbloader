package net.manmon.dbloader;

import java.io.Serializable;

public class TableInfo implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String name;
	private String start;
	private String end;

	public TableInfo() {
		
	}
	
	public TableInfo(String name, String start, String end) {
		this.name = name;
		this.start = start;
		this.end = end;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

}
