import java.util.Date;

/**
 * Created by lukasmohs on 13/02/17.
 */
public class LogEntry {

    private long id;
    private Date date;
    private String title;
    private String description;
    private int level;
    private String sensorStatus;

    public LogEntry(long id, Date date, String title, String description, int level, String sensorStatus) {
        this.id = id;
        this.date = date;
        this.title = title;
        this.description = description;
        this.level = level;
        this.sensorStatus = sensorStatus;
    }


    public long getId() {
        return id;
    }

    public Date getDate() {
        return date;
    }

    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }

    public int getLevel() {
        return level;
    }

    public String getSensorStatus(){ return sensorStatus; }
}
