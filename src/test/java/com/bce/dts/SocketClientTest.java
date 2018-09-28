package com.bce.dts;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.Socket;

public class SocketClientTest {
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("tc-dba-mdc-00.tc", 11907);
            // 由Socket对象得到输出流，并构造DataOutputStream对象
            DataOutputStream os = new DataOutputStream(socket.getOutputStream());
            // 由Socket对象得到输入流，并构造相应的BufferedReader对象
            BufferedReader is = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            // 将从系统标准输入读入的字符串输出到Server
            int magic = 0xdeadbabe;
            int type = 5;
            int lenght = 4;
            os.write(magic);
            os.write(lenght);
            os.write(type);
            // 刷新输出流，使Server马上收到该字符串
            os.flush();
            // 从Server读入一字符串，并打印到标准输出上
            System.out.println("Server:" + is.readLine());
            os.close(); // 关闭Socket输出流
            is.close(); // 关闭Socket输入流
            socket.close(); // 关闭Socket
        } catch (Exception e) {
            System.out.println("Error" + e); // 出错，则打印出错信息
        }
    }
}
