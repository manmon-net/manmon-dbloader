package net.manmon.dbloader;

import java.io.ByteArrayOutputStream;

public class DbData {
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    ByteArrayOutputStream strData = new ByteArrayOutputStream();

    public DbData() {

    }

    public DbData(ByteArrayOutputStream data, ByteArrayOutputStream strData) {
        this.data = data;
        this.strData = strData;
    }

    public ByteArrayOutputStream getData() {
        return data;
    }

    public void setData(ByteArrayOutputStream data) {
        this.data = data;
    }

    public ByteArrayOutputStream getStrData() {
        return strData;
    }

    public void setStrData(ByteArrayOutputStream strData) {
        this.strData = strData;
    }
}
