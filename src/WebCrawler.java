import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;


import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebCrawler {
    // DS for the crawler
    private Queue<String> URLsToVisit; // Queue of URLs to visit as a set so no dupes even on the same page
    private Set<String> visitedURLs; // Set of already visited URLs
    private Map<String, EmailRecord> emailsToWriteToDB; // Map of emails to write to DB being temp stored before writing
    private Set<String> uniqueEmails; // Set of unique emails found even already written to DB for easy checks
    private static int emailCount = 0; // counter for emails to log them as I get them

    //Start
    private final String startingURL = "https://www.touro.edu/"; // Default starting URL, can be changed
    private static final int EMAIL_BATCH = 500; // write to DB in batches of 500 emails

    //DB connection and regex for emails
    private Connection dbConnectionUrl; // Connection to the database
    private Pattern emailRegexPattern; // Regex pattern for matching emails

    public void crawl() throws SQLException, IOException {
        //initialize our DSs
        URLsToVisit = new ConcurrentLinkedQueue<>(); //should behave like a queue, but also keep insertion order
        visitedURLs = new HashSet<>();
        emailsToWriteToDB = new HashMap<>();
        uniqueEmails = new HashSet<>();

        // Set the email regex pattern
        emailRegexPattern = Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");

        //add the DB connection
        loadDB();

        //start crawling from the starting URL
        URLsToVisit.offer(startingURL);

        //main loop we use to crawl the webpages
        while (!URLsToVisit.isEmpty()) {
            String currentURL = URLsToVisit.poll(); //get the next url to visit

            //skip if already visited
            if (visitedURLs.contains(currentURL)) {
                continue;
            }

            //add to the visited URLs set
            visitedURLs.add(currentURL);

            try {
                //get the webpage
                Document webPage = Jsoup.connect(currentURL).timeout(60000).get();

                //get all the links on the current page
                getAndQueueNewLinks(webPage//,currentURL
                         );

                //get all the emails on the current page
                getAndStoreEmails(webPage, currentURL);

            } catch (IOException e) {
                //log the error and continue
                System.err.println("Error fetching URL: " + currentURL + " - " + e.getMessage());
            }
        }
    }

    private void writeEmailsToDB() throws SQLException {
        String insertQuery = "INSERT INTO Emails (emailAddress, source, timeStamp) VALUES (?, ?, ?)";
        try (PreparedStatement statement = dbConnectionUrl.prepareStatement(insertQuery)) {
            // batch the emails to write to DB
            for (Map.Entry<String, EmailRecord> entry : emailsToWriteToDB.entrySet()) {
                String emailAddress = entry.getKey();
                EmailRecord emailRecord = entry.getValue();

                // set the parameters for the prepared statement
                statement.setString(1, emailAddress);
                statement.setString(2, emailRecord.getSourceURL());
                statement.setTimestamp(3, Timestamp.valueOf(emailRecord.getDateTime()));

                // add to the batch
                statement.addBatch();


            }
            // put the batch in the DB
            statement.executeBatch();

            // add to the unique emails set
            uniqueEmails.addAll(emailsToWriteToDB.keySet());
            // clear the emails to write to DB map
            emailsToWriteToDB.clear();
        }

    }

    private void getAndStoreEmails(Document webPage, String currentURL) throws SQLException {
        //get all the text on the page
        String pageText = webPage.text();

        //find all emails in the page text using the regex pattern
        Matcher emailMatcher = emailRegexPattern.matcher(pageText);

        while (emailMatcher.find()) {
            // get the email and convert to lowercase so that we can check them
            String foundEmail = emailMatcher.group().toLowerCase();

            //first check if it's in our unique emails set
            if (uniqueEmails.contains(foundEmail) || emailsToWriteToDB.containsKey(foundEmail)) {
                // if it is, we skip it
                continue;
            }

            //added to previous check, so commenting out
            //now we check if it is in our temp emails map
//            if (emailsToWriteToDB.containsKey(foundEmail)) {
//                //if it is, we skip it
//                continue;
//            }

            //if we got here, it means we have a new email
            LocalDateTime timestamp = LocalDateTime.now();
            //create a new EmailRecord object
            EmailRecord emailRecord = new EmailRecord(foundEmail, currentURL, timestamp);
            //add it to our emails to write to DB map
            emailsToWriteToDB.put(foundEmail, emailRecord);

            //log it
            System.out.println("Found new email: " + foundEmail + " on page: " + currentURL +
                    "| Emails found: " + ++emailCount);

            // Check if we need to write emails to the database
            if (emailsToWriteToDB.size() >= EMAIL_BATCH) {
                writeEmailsToDB();
            }
        }
    }

    private void getAndQueueNewLinks(Document webPage//, String currentURL
    ) {
        Elements links = webPage.select("a[href]");

        for (Element link : links) {
            String linkURL = link.absUrl("href"); // Get the absolute URL

            // add the link to the queue if it is not already visited or in our queue
            if ((linkURL.startsWith("http://") || linkURL.startsWith("https://"))
                    && !visitedURLs.contains(linkURL) && !URLsToVisit.contains(linkURL)) {
                URLsToVisit.offer(linkURL);
            }
        }
    }

    private  void loadDB() throws IOException {
        Properties properties = new Properties();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("database.properties")) {
            properties.load(in);
        }
        String endpoint = properties.getProperty("DB_URL");
        String dbName = properties.getProperty("DB_NAME");
        String user = properties.getProperty("DB_USER");
        String password = properties.getProperty("DB_PASSWORD");
        String connectionUrl = // specifies how to connect to the database
                "jdbc:sqlserver://" + endpoint + ";"
                        + "database=" + dbName + ";"
                        + "user=" + user + ";"
                        + "password=" + password + ";"
                        + "encrypt=true;"
                        + "trustServerCertificate=true;"
                        + "loginTimeout=30;";
        try { //(Connection connection = DriverManager.getConnection(connectionUrl);
             //Statement statement = connection.createStatement()) {
            dbConnectionUrl = DriverManager.getConnection(connectionUrl);
            System.out.println("Connected to the database successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}