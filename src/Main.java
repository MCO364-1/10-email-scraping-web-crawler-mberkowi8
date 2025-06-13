import java.io.IOException;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException, IOException {
        WebCrawler crawler = new WebCrawler(30);
        crawler.crawl();
    }
}
