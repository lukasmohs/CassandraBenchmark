import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by lukasmohs on 13/02/17.
 */
public class MySQLConnector {

    private static int NUMBEROFCOLUMS = 100;


    public void createLogsTable(Statement stmt) throws SQLException {
        String sql = "CREATE TABLE logs " +
                "(id INT NOT NULL AUTO_INCREMENT, " +
                " date DATE not NULL, " +
                " title VARCHAR(255), " +
                " description VARCHAR(255), " +
                " level INTEGER, " +
                " PRIMARY KEY ( id ))";

        stmt.executeUpdate(sql);
    }

    public void createLargeLogsTable(Statement stmt) throws SQLException {
        String sql = "CREATE TABLE logs " +
                "(id INT NOT NULL AUTO_INCREMENT, " +
                " date DATE not NULL, " +
                " title VARCHAR(255), " +
                " description VARCHAR(255), " +
                " level INTEGER, ";
        for(int i = 0; i< NUMBEROFCOLUMS; i++) {
            sql += " server" + i + " VARCHAR(255), ";
        }
        sql += " PRIMARY KEY ( id ))";

        stmt.executeUpdate(sql);
    }

    public void insertLog(Statement stmt, String title, String description, int level ) throws SQLException {

        String sql = "INSERT INTO logs VALUES"
                + " (NULL, CURDATE(), '"
                + title + "', '"
                + description + "', "
                + level + ")";
        stmt.executeUpdate(sql);

        stmt.executeUpdate(sql);
    }

    public void dropLogsTable(Statement stmt) throws SQLException {
        String sql = "DROP TABLE IF EXISTS logs";

        stmt.executeUpdate(sql);
    }

    public void dropLargeLogsTable(Statement stmt) throws SQLException {
        String sql = "DROP TABLE IF EXISTS largelogs";

        stmt.executeUpdate(sql);
    }

    public void queryLogById(Statement stmt, int id) throws SQLException {
        LogEntry entry = null;
        ResultSet rs = stmt.executeQuery("select * from logs where id=" + id);
        while(rs.next()){
            entry = new LogEntry(
                    rs.getLong(rs.findColumn("id")),
                    rs.getDate(rs.findColumn("date")),
                    rs.getString(rs.findColumn("title")),
                    rs.getString(rs.findColumn("description")),
                    rs.getInt(rs.findColumn("level")));
        }
        rs.close();
    }



}
