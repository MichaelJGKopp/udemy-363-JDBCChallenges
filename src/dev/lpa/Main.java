package dev.lpa;

import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;


public class Main {
  
  private static String USE_SCHEMA = "USE storefront"; // DDL statement to set database
  
  private static int MYSQL_DB_NOT_FOUND = 1049;
  
  public static void main(String[] args) {
    
    var dataSource = new MysqlDataSource();
    dataSource.setServerName("localhost");
    dataSource.setPort(3306);
    dataSource.setUser(System.getenv("MYSQL_USER"));
    dataSource.setPassword(System.getenv("MYSQL_PASS"));
    
    Scanner scanner = new Scanner(System.in);
    
    try (Connection conn = dataSource.getConnection()) {
      
      DatabaseMetaData metaData = conn.getMetaData();
      System.out.println(metaData.getSQLStateType());
      if (!checkSchema(conn)) {
        System.out.println("storefront schema does not exist");
        setUpSchema(conn);
      } else {
        insertOrder(conn, "rareItem");
        System.out.println("Enter order id to delete: ");
        deleteOrder(conn, scanner.nextInt());
        printOrder(conn);
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
  
  private static void setUpSchema(Connection conn) throws SQLException {
    
    String createSchema = "CREATE SCHEMA storefront";
    
    String createOrder = """
      CREATE TABLE storefront.order (
      order_id int NOT NULL AUTO_INCREMENT,
      order_date DATETIME NOT NULL,
      PRIMARY KEY (order_id)
      )""";
    
    String createOrderDetails = """
      CREATE TABLE storefront.order_details (
      order_detail_id int NOT NULL AUTO_INCREMENT,
      item_description text,
      order_id int DEFAULT NULL,
      PRIMARY KEY (order_detail_id),
      KEY FK_ORDERID (order_id),
      CONSTRAINT FK_ORDERID FOREIGN KEY (order_id)
      REFERENCES storefront.order (order_id) ON DELETE CASCADE
      )""";
    
    try (Statement statement = conn.createStatement()) {
      
      System.out.println("Creating storefront Database");
      statement.execute(createSchema);
      if (checkSchema(conn)) {
        statement.execute(createOrder);
        System.out.println("Successfully Created Order");
        statement.execute(createOrderDetails);
        System.out.println("Successfully Created Order Details");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
  
  private static void insertOrder(Connection conn, String itemDescription) throws SQLException {
    
    int orderId = -1;
    try (Statement statement = conn.createStatement()) {
      conn.setAutoCommit(false);
      
      // Insert entry into order table and get key orderId
      DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
      var now = LocalDateTime.now().format(dtf);
      String insertOrder = "INSERT INTO storefront.order (order_date) VALUES (%s)"
                             .formatted(statement.enquoteLiteral(now));
      System.out.println(insertOrder);
      
      statement.execute(insertOrder, Statement.RETURN_GENERATED_KEYS);
      ResultSet rs = statement.getGeneratedKeys();
      orderId = (rs != null && rs.next()) ? rs.getInt(1) : -1;
      System.out.println("Generated order ID = " + orderId);
      
      // Insert entry into order table and get key orderId
      String insertOrderDetails =
        "INSERT INTO storefront.order_details (item_description, order_id) VALUES (%s, %d)"
                             .formatted(statement.enquoteLiteral(itemDescription), orderId);
      System.out.println(insertOrderDetails);
      
      statement.execute(insertOrderDetails);
      conn.commit();
    } catch (SQLException e) {
      conn.rollback();
      e.printStackTrace();
      System.err.println("SQLException during transaction, rollback changes: " + e.getMessage());;
    }
    conn.setAutoCommit(true);
    printOrder(conn);
  }
  
  private static void printOrder(Connection conn) throws SQLException {
    
    try (Statement statement = conn.createStatement()) {
      System.out.println("===============================");
      String query = "SELECT * FROM storefront.order_summary";
      ResultSet rs = statement.executeQuery(query);
      ResultSetMetaData metaData =  rs.getMetaData();
      int columnCount = metaData.getColumnCount();
      
      for (int i = 1; i <= columnCount; i++) {
        System.out.printf("%-20s", metaData.getColumnLabel(i).toUpperCase());
      }
      System.out.println();
      while(rs.next()) {
        for (int i = 1; i <= columnCount; i++) {
          System.out.printf("%-20s", rs.getString(i));
        }
        System.out.println();
      }
    }
  }
  
  private static boolean deleteOrder(Connection conn, int orderId) {
    
    try (Statement statement = conn.createStatement()) {
      String deleteOrder = "DELETE FROM storefront.order WHERE order_id=%d"
                             .formatted(orderId);
      statement.execute(deleteOrder);
      return statement.getUpdateCount() > 1;
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
  }
}
