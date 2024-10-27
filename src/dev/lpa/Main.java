package dev.lpa;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;


public class Main {
  
  private static String USE_SCHEMA = "USE storefront"; // DDL statement to set database
  
  private static int MYSQL_DB_NOT_FOUND = 1049;
  
  public static void main(String[] args) {
    
    var dataSource = new MysqlDataSource();
    dataSource.setServerName("localhost");
    dataSource.setPort(3306);
    dataSource.setUser(System.getenv("MYSQL_USER"));
    dataSource.setPassword(System.getenv("MYSQL_PASS"));
    
    try (Connection conn = dataSource.getConnection()) {
      
      DatabaseMetaData metaData = conn.getMetaData();
      System.out.println(metaData.getSQLStateType());
      if (!checkSchema(conn)) {
        System.out.println("storefront schema does not exist");
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static boolean checkSchema(Connection conn) throws SQLException {
    
    try (Statement statement = conn.createStatement()) {
      statement.execute(USE_SCHEMA);
    } catch (SQLException e) {
      e.printStackTrace();
      System.err.println("SQLState: " + e.getSQLState());
      System.err.println("Error Code: " + e.getErrorCode());
      System.err.println("Message: " + e.getMessage());
      if (conn.getMetaData().getDatabaseProductName().equals("MySQL")
            && e.getErrorCode() == MYSQL_DB_NOT_FOUND) {
        return false;
      } else throw e;
    }
    return true;
  }
}
