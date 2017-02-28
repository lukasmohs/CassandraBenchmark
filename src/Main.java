import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class Main {

    private static final int NUMBEROFINSERTS = 1000;
    private static int NUMBEROFCOLUMNS = 1000;

    public static void main(String[] args) {

        try {
            final CassandraConnector cassandraClient = new CassandraConnector(NUMBEROFCOLUMNS);
            final MySQLConnector sqlClient = new MySQLConnector(NUMBEROFCOLUMNS);

            final String ipAddress = "localhost";
            final int port = 9042;

            final String sqlAddress = "localhost";
            final int sqlPort = 3306;

            cassandraClient.connect(ipAddress, port);

            cassandraClient.dropKeySpace(cassandraClient, "logs_keyspace");
            cassandraClient.createKeySpace(cassandraClient, "logs_keyspace");

            //CASSANDRA**********************

            cassandraClient.dropLargeLogsTable(cassandraClient,"logs_keyspace");
            cassandraClient.createLargeLogsTable(cassandraClient,"logs_keyspace");

            long startLargeInsert = System.nanoTime();
            for(int i = 0; i < NUMBEROFINSERTS; i++) {
                cassandraClient.insertLargeLogEntry(cassandraClient,"logs_keyspace", i , "someTitle",  "someDescription",  i, 50);
            }
            long endLargeInsert = System.nanoTime();

            int randomSensorId =0;
            Random rand = new Random();
            long startLargeFetch = System.nanoTime();
            for(int i = NUMBEROFINSERTS-1; i > 0; i--) {
                 randomSensorId = rand.nextInt(NUMBEROFCOLUMNS );
                cassandraClient.querylargelogsByIdandSensor(cassandraClient,"logs_keyspace", i, randomSensorId);
            }
            long endLargeFetch = System.nanoTime();

            cassandraClient.close();

            //MYSQL**********************

            Class.forName("com.mysql.jdbc.Driver");
            Connection con=DriverManager.getConnection(
                    "jdbc:mysql://" + sqlAddress + ":" + sqlPort+ "/benchmark","liferay","liferay");
            Statement stmt=con.createStatement();

            sqlClient.dropLargeLogsTable(stmt);
            sqlClient.createLargeLogsTable(stmt);

            long startLargeSQLInsert = System.nanoTime();
            for(int i = 0; i <NUMBEROFINSERTS; i++) {
                sqlClient.insertLargeLog(stmt, "someTitle",  "someDescription", i, 50);
            }
            long endSQLargeLInsert = System.nanoTime();

            long startLargeSQLQuery = System.nanoTime();
            for(int i = NUMBEROFINSERTS-1; i >0; i--) {
                randomSensorId = rand.nextInt(NUMBEROFCOLUMNS);
                sqlClient.queryLargeLogsById(stmt,i,randomSensorId);
            }
            long endLargeSQLQuery = System.nanoTime();

            //OUTPUT**********************

            System.out.println("Cassandra large insert duration: " + ((double) (endLargeInsert-startLargeInsert) / 1000000000.0 ));
            System.out.println("MYSQL large insert duration: " + ((double) (endSQLargeLInsert-startLargeSQLInsert) / 1000000000.0 ));
            System.out.println("--");
            System.out.println("Cassandra large fetch duration: " + ((double) (endLargeFetch - startLargeFetch) / 1000000000.0));
            System.out.println("MYSQL large query duration: " + ((double) (endLargeSQLQuery-startLargeSQLQuery) / 1000000000.0 ));

        } catch (ClassNotFoundException e) {
            System.out.println("Class not found");
        } catch (SQLException e) {
            System.out.println(e);
        }

    }


}
