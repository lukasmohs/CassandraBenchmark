import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class Main {

    private static final int NUMBEROFINSERTS = 1000;
    private static final int SIZEOFDESCRIPTION = 130;
    private static int NUMBEROFCOLUMNS = 200;

    public static void main(String[] args) {

        final CassandraConnector client = new CassandraConnector(NUMBEROFCOLUMNS);
        final MySQLConnector sqlClient = new MySQLConnector(NUMBEROFCOLUMNS);

        final String ipAddress = "localhost";
        final int port = 9042;
        client.connect(ipAddress, port);

        client.dropKeySpace(client, "logs_keyspace");
        client.createKeySpace(client, "logs_keyspace");
        client.dropLogsTable(client,"logs_keyspace");
        client.createLogsTable(client, "logs_keyspace");
        client.dropLargeLogsTable(client,"logs_keyspace");
        client.createLargeLogsTable(client,"logs_keyspace");


        long startInsert = System.nanoTime();
        for(int i = 0; i < NUMBEROFINSERTS; i++) {
            client.insertLogEntry(client,"logs_keyspace", i , "BLABLA",  new BigInteger(SIZEOFDESCRIPTION, new SecureRandom()).toString(32),  i);
        }
        long endInsert = System.nanoTime();

        long startFetch = System.nanoTime();
        for(int i = NUMBEROFINSERTS; i > 0; i--) {
            client.querylogsById(client,"logs_keyspace", "logs", i);
        }
        long endFetch = System.nanoTime();

        long startLargeInsert = System.nanoTime();
        for(int i = 0; i < NUMBEROFINSERTS; i++) {
            client.insertLargeLogEntry(client,"logs_keyspace", i , "BLABLA",  new BigInteger(SIZEOFDESCRIPTION, new SecureRandom()).toString(32),  i);
        }
        long endLargeInsert = System.nanoTime();

        int randomServerId =0;
        Random rand = new Random();
        long startLargeFetch = System.nanoTime();
        for(int i = NUMBEROFINSERTS-1; i > 0; i--) {
             randomServerId = rand.nextInt(NUMBEROFCOLUMNS );
            client.querylargelogsByIdandServer(client,"logs_keyspace", i, randomServerId);
        }
        long endLargeFetch = System.nanoTime();


        client.close();

        try {

            Class.forName("com.mysql.jdbc.Driver");
            Connection con=DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/benchmark","liferay","liferay");
            Statement stmt=con.createStatement();

            sqlClient.dropLogsTable(stmt);
            sqlClient.createLogsTable(stmt);


            long startSQLInsert = System.nanoTime();
            for(int i = 0; i <NUMBEROFINSERTS; i++) {
                sqlClient.insertLog(stmt, "BLABLA",  new BigInteger(SIZEOFDESCRIPTION, new SecureRandom()).toString(32), i);
            }
            long endSQLInsert = System.nanoTime();


            long startSQLQuery = System.nanoTime();
            for(int i = NUMBEROFINSERTS; i >0; i--) {
                sqlClient.insertLog(stmt, "BLABLA",  new BigInteger(SIZEOFDESCRIPTION, new SecureRandom()).toString(32), i);
            }
            long endSQLQuery = System.nanoTime();

            sqlClient.dropLargeLogsTable(stmt);
            sqlClient.createLargeLogsTable(stmt);

            long startLargeSQLInsert = System.nanoTime();
            for(int i = 0; i <NUMBEROFINSERTS; i++) {
                sqlClient.insertLargeLog(stmt, "BLABLA",  new BigInteger(SIZEOFDESCRIPTION, new SecureRandom()).toString(32), i);
            }
            long endSQLargeLInsert = System.nanoTime();


            long startLargeSQLQuery = System.nanoTime();
            for(int i = NUMBEROFINSERTS-1; i >0; i--) {
                randomServerId = rand.nextInt(NUMBEROFCOLUMNS);
                sqlClient.queryLargeLogsById(stmt,i,randomServerId);
            }
            long endLargeSQLQuery = System.nanoTime();

            System.out.println("Cassandra insert duration: " + ((double) (endInsert-startInsert) / 1000000000.0 ));
            System.out.println("MYSQL insert duration: " + ((double) (endSQLInsert-startSQLInsert) / 1000000000.0 ));
            System.out.println("--");
            System.out.println("Cassandra fetch duration: " + ((double) (endFetch - startFetch) / 1000000000.0));
            System.out.println("MYSQL fetch duration: " + ((double) (endSQLQuery-startSQLQuery) / 1000000000.0 ));
            System.out.println("--");
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
