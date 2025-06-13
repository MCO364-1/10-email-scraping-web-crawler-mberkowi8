import java.time.LocalDateTime;

public class EmailRecord {
    private String email;
    private String sourceURL;
    private LocalDateTime timestamp;

    //constructor for new email record
    public EmailRecord(String email, String source, LocalDateTime timestamp) {
        this.email = email;
        this.sourceURL = source;
        this.timestamp = timestamp;
    }

    //getters
    public String getEmail() {
        return email;
    }

    public String getSourceURL() {
        return sourceURL;
    }

    public LocalDateTime getDateTime() {
        return timestamp;
    }


}
