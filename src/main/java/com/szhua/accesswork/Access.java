package com.szhua.accesswork;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Access implements Writable {


    String ip ;
    long up ;
    long down ;
    long sum ;

    public Access() {

    }


    public Access(String ip, long up, long down) {
        this.ip = ip;
        this.up = up;
        this.down = down;
        this.sum = up+down;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(ip);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.ip = in.readUTF();
        this.up =in.readLong();
        this.down =in.readLong() ;
        this.sum =in.readLong();
    }

    @Override
    public String toString() {
        return ip+"\t"+up+"\t"+down+"\t"+sum;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }
}
