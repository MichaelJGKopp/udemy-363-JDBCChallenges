package dev.lpa;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class Main {
  
  public static void main(String[] args) {
    
    var dataSource = new MysqlDataSource();
    dataSource.setServerName("localhost");
    dataSource.setPort(3306);
    dataSource.setDatabaseName("music");
    try {
      dataSource.setMaxRows(10);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    
    try (Connection connection = dataSource.getConnection(
      System.getenv("MYSQL_USER"), System.getenv("MYSQL_PASS"));
         var statement = connection.createStatement()) {
        var result = statement.executeQuery("SELECT * FROM music.albumview");
        while (result.next()) {
          String data = result.getString(1);
          System.out.println(data);
        }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
