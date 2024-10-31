package dev.lpa;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ChallengePreparedStatement {
  
  public static void main(String[] args) {
    
    var dataSource = new MysqlDataSource();
    dataSource.setServerName("localhost");
    dataSource.setPort(3306);
    dataSource.setDatabaseName("storefront");
    
    try (Connection connection = dataSource.getConnection(
      System.getenv("MYSQL_USER"), System.getenv("MYSQL_PASS"))
    ) {
//      addColumn(connection,
//        "storefront.order_details", "quantity", "INTEGER");
      
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static void addColumn(Connection connection,
                               String tableName, String colName, String colType)
    throws SQLException {
    
    try (Statement statement = connection.createStatement()) {
      
      String query = "ALTER TABLE %s ADD COLUMN %s %s;".formatted(tableName, colName, colType);
      statement.execute(query);
      System.out.println(query);
    } catch (SQLException e) {
      System.err.println("Failed to add column: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
