import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * Created by lukasmohs on 13/02/17.
 */
public class MySQLConnector {

    private int NUMBEROFCOLUMNS = 0;

    public MySQLConnector(int numberOfColumns) {
        NUMBEROFCOLUMNS = numberOfColumns;
    }


    public void createLargeLogsTable(Statement stmt) throws SQLException {
        String sql = "CREATE TABLE largelogs " +
                "(id INT NOT NULL AUTO_INCREMENT, " +
                " date DATE not NULL, " +
                " title VARCHAR(31), " +
                " description VARCHAR(31), " +
                " level INTEGER, ";
        for(int i = 0; i< NUMBEROFCOLUMNS; i++) {
            sql += " sensor" + i + " VARCHAR(1), ";
        }
        sql += " PRIMARY KEY ( id ))";

        stmt.executeUpdate(sql);
    }

    public void insertLargeLog(Statement stmt, String title, String description, int level, int densityInPercent ) throws SQLException {

        String sql = "INSERT INTO largelogs ("
                + "id, date, title, description, level";

        int factor = 100/densityInPercent;
        for(int i = 0; i<NUMBEROFCOLUMNS; i++){
            if(i%factor == 0) {
                sql += ", sensor" + i;
            }
        }

        sql     += ") VALUES(NULL, CURDATE(), '"
                + title + "', '"
                + description + "', "
                + level;

        Random r = new Random();

        for(int i = 0; i< NUMBEROFCOLUMNS; i++) {
            if(i%factor == 0) {
                sql += ", '" +  (char)(r.nextInt(26) + 'a') + "'";
                //sql += " ,'" + new BigInteger(130, new SecureRandom()).toString(32) + "'";
            }
        }
        sql += ")";

        stmt.executeUpdate(sql);
    }


    public void dropLargeLogsTable(Statement stmt) throws SQLException {
        String sql = "DROP TABLE IF EXISTS largelogs";

        stmt.executeUpdate(sql);
    }

    public void queryLargeLogsById(Statement stmt, int id, int sensorId) throws SQLException {
        LogEntry entry = null;
        ResultSet rs = stmt.executeQuery("select * from largelogs where id=" + id);
        while(rs.next()){
            entry = new LogEntry(
                    rs.getLong(rs.findColumn("id")),
                    rs.getDate(rs.findColumn("date")),
                    rs.getString(rs.findColumn("title")),
                    rs.getString(rs.findColumn("description")),
                    rs.getInt(rs.findColumn("level")),
                    rs.getString(rs.findColumn("sensor" + sensorId)));
        }
        rs.close();
    }
}
