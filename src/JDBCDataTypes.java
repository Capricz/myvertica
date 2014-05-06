import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDBCDataTypes {
    public static void main(String[] args) {
    	// If running under a Java 5 JVM, use you need to load the JDBC driver
    	// using Class.forname here
    	
        Properties myProp = new Properties();
        myProp.put("user", "dbadmin");
        myProp.put("password", "vertica123");

        Connection conn;
        try {
            conn = DriverManager.getConnection("jdbc:vertica://C0045453.itcs.hp.com:5433/Vertica",myProp);

            Statement statement = conn.createStatement();

            // Create a table that will hold a row of different types of
            // numeric data.
            statement.executeUpdate(
            		"DROP TABLE IF EXISTS test_all_types cascade");

            statement.executeUpdate("CREATE TABLE test_all_types ("
                            + "c0 INTEGER, c1 TINYINT, c2 DECIMAL, "
                            + "c3 MONEY, c4 DOUBLE PRECISION, c5 REAL)");

            // Add a row of values to it.
            statement.executeUpdate("INSERT INTO test_all_types VALUES("
                            + "111111111111, 444, 55555555555.5555, "
                            + "77777777.77,  88888888888888888.88, " 
                            + "10101010.10101010101010)");

            // Query the new table to get the row back as a result set.
            ResultSet rs = statement
                            .executeQuery("SELECT * FROM test_all_types");

            // Get the metadata about the row, including its data type.
            ResultSetMetaData md = rs.getMetaData();

            // Loop should only run once...
            while (rs.next()) {
                // Print out the data type used to defined the column, followed
                // by the values retrieved using several different retrieval
                // methods.
            	
            	String[] vertTypes = new String[] {"INTEGER", "TINYINT",
            			 "DECIMAL", "MONEY", "DOUBLE PRECISION", "REAL"};
            	
            	for (int x=1; x<7; x++) { 
            		System.out.println("\n\nColumn " + x + " (" + vertTypes[x-1]
            				+ ")");
            		System.out.println("\tgetColumnType()\t\t"
                            + md.getColumnType(x));
            		System.out.println("\tgetColumnTypeName()\t"
                            + md.getColumnTypeName(x));
                    System.out.println("\tgetShort()\t\t"
                            + rs.getShort(x)); 
                    System.out.println("\tgetLong()\t\t" + rs.getLong(x));
                    System.out.println("\tgetInt()\t\t" + rs.getInt(x));

                    System.out.println("\tgetByte()\t\t" + rs.getByte(x));                    
            	}
            }
            rs.close();
            statement.executeUpdate("drop table test_all_types cascade");
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}