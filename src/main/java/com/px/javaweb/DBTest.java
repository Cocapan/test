package com.px.javaweb;


import java.sql.Connection;
import java.sql.DriverManager;

public class DBTest {

    public static final String url = "jdbc:mysql://172.16.13.249/my_test";
    public static final String name = "com.mysql.jdbc.Driver";
    public static final String user = "root";
    public static final String password = "root";


    public static void main(String[] args) {
        try {
            Class.forName(name);
            Connection conn = DriverManager.getConnection(url, user, password);
            System.out.println(conn);
        } catch (Exception  e) {
            e.printStackTrace();
        }
    }

}
