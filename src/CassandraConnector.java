/**
 * Created by lukasmohs on 13/02/17.
 */

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Date;

/**
 * Class used for connecting to Cassandra database.
 */
public class CassandraConnector
{

    private static int NUMBEROFCOLUMNS = 0;
    /** Cassandra Cluster. */
    private Cluster cluster;
    /** Cassandra Session. */
    private Session session;

    public CassandraConnector(int numberOfColumns) {
        NUMBEROFCOLUMNS = numberOfColumns;
    }


    public void connect(final String node, final int port)
    {
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

    public  void dropLogsTable(CassandraConnector client, String keySpace) {
        client.getSession().execute( "DROP TABLE IF EXISTS " + keySpace + ".logs");
    }

    public  void createLogsTable(CassandraConnector client, String keySpace) {
        client.getSession().execute("CREATE TABLE " + keySpace + ".logs (id int, date timestamp, title text, description text, "
                + "level int, PRIMARY KEY (id))");
    }

    public  void dropLargeLogsTable(CassandraConnector client, String keySpace) {
        client.getSession().execute( "DROP TABLE IF EXISTS " + keySpace + ".largelogs");
    }

    public  void createLargeLogsTable(CassandraConnector client, String keySpace) {
        String sql = "CREATE TABLE " + keySpace + ".largelogs (id int, date timestamp, title text, description text, level int, ";

        for(int i = 0; i<NUMBEROFCOLUMNS; i++) {
            sql += "server" + i + " text,";
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

    public  void insertLogEntry(CassandraConnector client, String keySpace, int id, String title,  String description, int level) {
        Insert insert = QueryBuilder.insertInto(keySpace, "logs")
                .value("id", id)
                .value("date", System.nanoTime())
                .value("title", title)
                .value("description", description)
                .value("level", level);


        client.getSession().execute(insert.toString());
    }

    public  void insertLargeLogEntry(CassandraConnector client, String keySpace, int id, String title,  String description, int level) {
        Insert insert = QueryBuilder.insertInto(keySpace, "largelogs")
                .value("id", id)
                .value("date", System.nanoTime())
                .value("title", title)
                .value("description", description)
                .value("level", level);


        for(int i = 0; i<NUMBEROFCOLUMNS; i++){
            insert.value("server" + i, new BigInteger(130, new SecureRandom()).toString(32));
        }

        client.getSession().execute(insert.toString());
    }

    public LogEntry querylogsById(CassandraConnector client, String keySpace, String tableName, int id) {

        Select.Where select = QueryBuilder.select("id", "date", "title", "description", "level")
                .from(keySpace,tableName)
                .where(QueryBuilder.eq("id", id));

        ResultSet logEntryResults = client.getSession().execute(select.toString().substring(0, select.toString().length()-1) + " ALLOW FILTERING");
        final Row logEntryRow = logEntryResults.one();
        LogEntry logEntry = null;
        if(logEntryRow!=null) {
            logEntry = new LogEntry(
                    logEntryRow.getInt("id"),
                    new Date(logEntryRow.getTimestamp("date").getTime()),
                    logEntryRow.getString("title"),
                    logEntryRow.getString("description"),
                    logEntryRow.getInt("level"));
        }

        return logEntry;
    }

    public LogEntry querylargelogsByIdandServer(CassandraConnector client, String keySpace, int id, int serverId) {

        Select.Where select = QueryBuilder.select("id", "date", "title", "description", "level", "server"+serverId)
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
                    logEntryRow.getString("server" + serverId));
        } else {
            System.out.println("no matching row found");
        }

        return logEntry;
    }
}
