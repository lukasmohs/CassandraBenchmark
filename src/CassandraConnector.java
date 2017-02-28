/**
 * Created by lukasmohs on 13/02/17.
 */

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import java.util.Date;
import java.util.Random;

public class CassandraConnector
{

    private static int NUMBEROFCOLUMNS = 0;
    private Cluster cluster;
    private Session session;

    public CassandraConnector(int numberOfColumns) {
        NUMBEROFCOLUMNS = numberOfColumns;
    }


    public void connect(final String node, final int port) {
        this.cluster = Cluster.builder().addContactPoint(node).withPort(port)
                .withProtocolVersion(ProtocolVersion.V4).build();
        session = cluster.connect();
    }

    public Session getSession()
    {
        return this.session;
    }

    public void close()
    {
        cluster.close();
    }


    public  void dropLargeLogsTable(CassandraConnector client, String keySpace) {
        client.getSession().execute( "DROP TABLE IF EXISTS " + keySpace + ".largelogs");
    }

    public  void createLargeLogsTable(CassandraConnector client, String keySpace) {
        String sql = "CREATE TABLE " + keySpace + ".largelogs (id int, date timestamp, title text, description text, level int, ";

        for(int i = 0; i<NUMBEROFCOLUMNS; i++) {
            sql += "sensor" + i + " varchar,";
        }
        sql += " PRIMARY KEY (id))";
        client.getSession().execute(sql);
    }

    public  void createKeySpace(CassandraConnector client, String keySpaceName) {
        client.getSession().execute( "CREATE KEYSPACE " + keySpaceName
                + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");

    }

    public  void dropKeySpace(CassandraConnector client, String keySpaceName) {
        client.getSession().execute( "DROP KEYSPACE IF EXISTS " + keySpaceName);

    }


    public  void insertLargeLogEntry(CassandraConnector client, String keySpace, int id, String title,  String description, int level, int densityInPercent) {
        Insert insert = QueryBuilder.insertInto(keySpace, "largelogs")
                .value("id", id)
                .value("date", System.nanoTime())
                .value("title", title)
                .value("description", description)
                .value("level", level);


        Random r = new Random();
        int factor = 100/densityInPercent;
        for(int i = 0; i<NUMBEROFCOLUMNS; i++){
            if(i%factor == 0) {
                insert.value("sensor" + i, (char) (48 + r.nextInt(47)) + "");
                //insert.value("sensor" + i, new BigInteger(130, new SecureRandom()).toString(32));
            }
        }

        client.getSession().execute(insert.toString());
    }


    public LogEntry querylargelogsByIdandSensor(CassandraConnector client, String keySpace, int id, int sensorId) {

        Select.Where select = QueryBuilder.select("id", "date", "title", "description", "level", "sensor"+sensorId)
                .from(keySpace,"largelogs")
                .where(QueryBuilder.eq("id", id));

        ResultSet logEntryResults = client.getSession().execute(select);
        final Row logEntryRow = logEntryResults.one();
        LogEntry logEntry = null;
        if(logEntryRow!=null) {
            logEntry = new LogEntry(
                    logEntryRow.getInt("id"),
                    new Date(logEntryRow.getTimestamp("date").getTime()),
                    logEntryRow.getString("title"),
                    logEntryRow.getString("description"),
                    logEntryRow.getInt("level"),
                    logEntryRow.getString("sensor" + sensorId));
        } else {
            System.out.println("no matching row found");
        }

        return logEntry;
    }
}
