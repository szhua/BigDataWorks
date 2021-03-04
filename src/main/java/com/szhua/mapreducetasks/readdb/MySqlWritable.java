package com.szhua.mapreducetasks.readdb;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import scala.Char;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlWritable  implements DBWritable, Writable {


    int deptno ;
    String  dname ;
    String loc ;

    /**
     * === Writable implements methods
     */

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(deptno);
        out.writeUTF(dname);
        out.writeUTF(loc);

    }

    @Override
    public String toString() {
        return "MySqlWritable{" +
                "deptno=" + deptno +
                ", dname='" + dname + '\'' +
                ", loc='" + loc + '\'' +
                '}';
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.deptno = in.readInt();
        this.dname =in.readUTF();
        this.loc =in.readUTF();
    }


    @Override
    public void write(PreparedStatement statement) throws SQLException {
          statement.setInt(1,deptno);
          statement.setString(2,dname);
          statement.setString(3,loc);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
          this.deptno = resultSet.getInt(1);
          this.dname =resultSet.getString(2);
          this.loc =resultSet.getString(3);
    }


    public static void main(String[] args) {
        //         1100
        //         0111
        System.err.println(130&127);
    }
}
