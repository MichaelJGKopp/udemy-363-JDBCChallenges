package dev.lpa;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


public class Main {
  
  private static String USE_SCHEMA = "USE storefront"; // DDL statement to set database
  
  public static void main(String[] args) {
    
    var dataSource = new MysqlDataSource();
    dataSource.setServerName("localhost");
    dataSource.setPort(3306);
    dataSource.setUser(System.getenv("MYSQL_USER"));
    dataSource.setPassword(System.getenv("MYSQL_PASS"));
  }
  
  private static boolean checkSchema(Connection conn) {
    
    try (Statement statement = conn.createStatement()) {
      statement.execute(USE_SCHEMA);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }
}
