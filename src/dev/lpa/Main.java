package dev.lpa;

import com.mysql.cj.jdbc.MysqlDataSource;

public class Main {
  
  private static String USE_SCHEMA = "USE storefront"; // DDL statement to set database
  
  public static void main(String[] args) {
    
    var dataSource = new MysqlDataSource();
    dataSource.setServerName("localhost");
    dataSource.setPort(3306);
    dataSource.setUser(System.getenv("MYSQL_USER"));
    dataSource.setPassword(System.getenv("MYSQL_PASS"));
  }
}
