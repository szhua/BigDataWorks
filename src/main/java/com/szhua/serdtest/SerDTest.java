package com.szhua.serdtest;

import java.io.*;
import java.nio.file.FileSystem;

public class SerDTest {


    public static void main(String[] args) throws  Exception{


        String path  = "/Users/szhua/Desktop/ww.txt";

        DataOutputStream dataOutputStream =new DataOutputStream(new FileOutputStream(path));
        dataOutputStream.writeUTF("哈哈哈"); // UTF-8  每个字符占用了3个byte   9+2=11
           dataOutputStream.writeUTF("Ac"); // UTF-8 英文占用                    2个byte 2+2=4
           dataOutputStream.writeInt(20); //32位的int 占用了                     4 byte
        //19

//        dataOutputStream.writeLong(11); //64位的long 占用了                   8 byte   23
//        dataOutputStream.writeByte(2); //8位的byte 占用                       1  byte  24
//        dataOutputStream.writeShort(1);//16位的short占用了                    2 byte    26
//        dataOutputStream.writeFloat(111f);//32float                         4 byte    30
//        dataOutputStream.writeChar(111);  //16 位                            2 byte    32



        dataOutputStream.close();


      //  DataInputStream dataInputStream =new DataInputStream(new FileInputStream(path));



    }
}
