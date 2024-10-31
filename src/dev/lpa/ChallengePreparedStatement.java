package dev.lpa;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ChallengePreparedStatement {
  
  private static String ORDER_INSERT =
    "INSERT INTO storefront.order (order_date) VALUES (?)";
  private static String ORDER_DETAILS_INSERT =
    "INSERT INTO storefront.order_details (order_id, quantity, item_description) VALUES (?, ?, ?)";
  
  public static void main(String[] args) {
    
    var dataSource = new MysqlDataSource();
    dataSource.setServerName("localhost");
    dataSource.setPort(3306);
    dataSource.setDatabaseName("storefront");
    try {
      dataSource.setContinueBatchOnError(false);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    
    try (Connection connection = dataSource.getConnection(
      System.getenv("MYSQL_USER"), System.getenv("MYSQL_PASS"))
    ) {
//      addColumn(connection,
//        "storefront.order_details", "quantity", "INTEGER");
      Path filePath = Path.of("Orders.csv");
      try {
        addOrdersFromCSV(connection, filePath);
      } catch (IOException e) {
        System.err.println("Could not load " + filePath.toAbsolutePath() + ": " + e.getMessage());
      }
      
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static void addColumn(Connection connection,
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
  
  private static void addOrdersFromCSV(Connection conn, Path filePath) throws IOException, SQLException {
    
    List<String> lines = Files.readAllLines(filePath);
    
    try (
      PreparedStatement psOrder = conn.prepareStatement(ORDER_INSERT,
        Statement.RETURN_GENERATED_KEYS);
      PreparedStatement psOrderDetails = conn.prepareStatement(ORDER_DETAILS_INSERT)
    ) {
      conn.setAutoCommit(false);
      
      String orderDate = null;
      List<String[]> items = new ArrayList<>();
      String itemDescription = null;
      String itemQuantity = null;
      int orderId;
      
      for (String line : lines) {
        String[] cells = line.split(",");
        if (cells[0].trim().equalsIgnoreCase("order")) {
          if (orderDate != null) {
            orderId = addOrder(psOrder, orderDate);
            for (var item : items) {
              addItem(psOrderDetails, orderId, Integer.parseInt(item[0]), item[1]);
            }
            items.clear();
          }
          orderDate = cells[1];
        } else if (cells[0].trim().equalsIgnoreCase("item")) {
          itemQuantity = cells[1].trim();
          itemDescription = cells[2].trim();
          items.add(new String[]{itemQuantity, itemDescription});
        }
      }
      
      orderId = addOrder(psOrder, orderDate);
      for (var item : items) {
        addItem(psOrderDetails, orderId, Integer.parseInt(item[0]), item[1]);
      }
      
      psOrderDetails.executeBatch();
      conn.commit();
    } catch (SQLException e) {
      System.err.println("Could not add Orders from CSV: " + e.getMessage());
      e.printStackTrace();
      conn.rollback();
    } finally {
      conn.setAutoCommit(true);
    }
  }
  
  private static int addOrder(PreparedStatement psOrder, String orderDate) throws SQLException {
    
    int orderId = -1;
    psOrder.setTimestamp(1, Timestamp.valueOf(orderDate));
    
    int inserted = psOrder.executeUpdate();
    if (inserted > 0) {
      ResultSet keysGenerated = psOrder.getGeneratedKeys();
      if (keysGenerated.next()) {
        orderId = keysGenerated.getInt(1);
      }
    }
    
    return orderId;
  }
  
  private static void addItem(PreparedStatement psItem, int orderId, int itemQty,
                              String itemDescr) throws SQLException {
    
    psItem.setInt(1, orderId);
    psItem.setInt(2, itemQty);
    psItem.setString(3, itemDescr);
    
    psItem.addBatch();
  }
}
