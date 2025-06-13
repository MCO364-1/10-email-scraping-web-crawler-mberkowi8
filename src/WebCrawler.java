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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebCrawler {
    // DS for the crawler
    private Queue<String> URLsToVisit; // Queue of URLs to visit as a set so no dupes even on the same page
    private Set<String> visitedURLs; // Set of already visited URLs
    private Map<String, EmailRecord> emailsToWriteToDB; // Map of emails to write to DB being temp stored before writing
    private Set<String> uniqueEmails; // Set of unique emails found even already written to DB for easy checks
    private static AtomicInteger emailCount = new AtomicInteger(0); // counter for emails to log them as I get them

    //set up multithreading
    private final ExecutorService threadPool;
    private final int numThreads;
    private final AtomicInteger threadsRunning = new AtomicInteger(0);
    private volatile boolean stopCrawling = false;


    //Start
    private final String startingURL = "https://www.touro.edu/"; // Default starting URL, can be changed
    private static final int EMAIL_BATCH = 500; // write to DB in batches of 500 emails
    private final int TARGET_EMAILS = 10_000;

    //DB connection and regex for emails
    private Connection dbConnectionUrl; // Connection to the database
    private Pattern emailRegexPattern; // Regex pattern for matching emails

    private final Object dbLock = new Object();
    private final Object emailLock = new Object();

    public WebCrawler(int numThreads) {
        this.numThreads = numThreads;
        this.threadPool = Executors.newFixedThreadPool(numThreads);

        //initialize the threadsafe DSs
        URLsToVisit = new ConcurrentLinkedQueue<>(); //should behave like a queue, but also keep insertion order
        visitedURLs = Collections.synchronizedSet(new HashSet<>());
        emailsToWriteToDB = Collections.synchronizedMap(new HashMap<>());
        uniqueEmails = Collections.synchronizedSet(new HashSet<>());

        System.out.println("Webcrawler with " + numThreads);
    }

    public void crawl() throws SQLException, IOException {
        // Set the email regex pattern
        emailRegexPattern = Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");

        //add the DB connection
        loadDB();

        //start crawling from the starting URL
        URLsToVisit.offer(startingURL);

        //create threads
        for (int i = 0; i < numThreads; i++) {
            // number threads for logging
            final int workerNum = i + 1;
            threadPool.submit(() -> {
                System.out.println("Thread #:" + workerNum + ", started.");
                threadsRunning.incrementAndGet();

                try {
                    while (!stopCrawling && emailCount.get() < TARGET_EMAILS) {
                        String currentURL = null;

                        try {
                            //get URLs from the queue
                            currentURL = URLsToVisit.poll();

                            if (currentURL == null) {
                                //if nothing to get then wait
                                Thread.sleep(30000); // wait for 1 second before checking again
                                continue;
                            }

                            if (!visitedURLs.add(currentURL)) {
                                // if the URL was already visited, skip it
                                continue;
                            }

                            System.out.println("Thread #" + workerNum + " visiting: " + currentURL);

                            //get the webpage
                            Document webPage = Jsoup.connect(currentURL).timeout(60000).get();

                            //get all the links on the current page
                            getAndQueueNewLinks(webPage);

                            //get all the emails on the current page
                            getAndStoreEmails(webPage, currentURL);
                        } catch (IOException e) {
                            //log the error and continue
                            System.err.println("Thread #: " + workerNum + ". Error fetching URL: " + currentURL + " - " + e.getMessage());
                        } catch (SQLException e) {
                            //log the error and continue
                            System.err.println("Thread #: " + workerNum + ". DB error: " + e.getMessage());
                        } catch (InterruptedException e) {
                            // if interrupted, stop crawling
                            Thread.currentThread().interrupt();
                            //stop right away
                            break;
                        }

                        // Check if we need reached the email target
                        if (emailCount.get() >= TARGET_EMAILS) {
                            System.out.println("Thread #" + workerNum + " reached the target: " + TARGET_EMAILS + " emails.");
                            stopCrawling = true;
                            break;
                        }
                    }
                } finally {
                    threadsRunning.decrementAndGet();
                    System.out.println("Thread #" + workerNum + " finished.");
                }
            });
        }

        //monitor the threads
        monitorThreads();
        //shutdown
        shutdown();
    }

    private void shutdown() throws SQLException {
        System.out.println("Shutting down crawler...");

        stopCrawling = true;
        threadPool.shutdown();

        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
                if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.err.println("Thread pool didn't stop");
                }
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Write any remaining emails to database
        synchronized (dbLock) {
            if (!emailsToWriteToDB.isEmpty()) {
                writeEmailsToDB();
            }
        }

        System.out.println("Crawling completed. Total emails found: " + emailCount.get());
    }

    private void monitorThreads() {
        try {
            while (threadsRunning.get() > 0 && !stopCrawling && emailCount.get() < TARGET_EMAILS) {
                //check every 5 secs
                Thread.sleep(10000);

                System.out.println("Progress: " + emailCount.get() + " emails found, " +
                                 visitedURLs.size() + " URLs visited, " +
                                 URLsToVisit.size() + " URLs queued, " +
                                 threadsRunning.get() + " threads running");

                //check if threads aren't doing anything and no more URLs to visit
                if (URLsToVisit.isEmpty() && threadsRunning.get() > 0) {
                    //wait to see if we get more URLs
                    Thread.sleep(10_000);
                    //if still no URLs to visit, we can stop crawling
                    if (URLsToVisit.isEmpty()) {
                        System.out.println("No more URLs to visit. Stopping...");
                        stopCrawling = true;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Monitoring interrupted, stopping crawling.");
        }
    }

    private void writeEmailsToDB() throws SQLException {
        String insertQuery = "INSERT INTO Emails (emailAddress, source, timeStamp) VALUES (?, ?, ?)";
        try (PreparedStatement statement = dbConnectionUrl.prepareStatement(insertQuery)) {
            int batchSize = 0; // to keep track of the batch size

            // batch the emails to write to DB
            for (Map.Entry<String, EmailRecord> entry : emailsToWriteToDB.entrySet()) {
                if (batchSize >= EMAIL_BATCH) {
                    //stop if we reached the batch size
                    break;
                }

                String emailAddress = entry.getKey();
                EmailRecord emailRecord = entry.getValue();

                // set the parameters for the prepared statement
                statement.setString(1, emailAddress);
                statement.setString(2, emailRecord.getSourceURL());
                statement.setTimestamp(3, Timestamp.valueOf(emailRecord.getDateTime()));

                // add to the batch
                statement.addBatch();
                batchSize++;
            }
            // put the batch in the DB
            statement.executeBatch();
            System.out.println("Inserted " + batchSize + " emails into the database.");

            // add to the unique emails set
            //uniqueEmails.addAll(emailsToWriteToDB.keySet());
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

            //threadsafe check for the email
            synchronized (emailLock) {
                //first check if it's in our unique emails set
                if (uniqueEmails.contains(foundEmail) || emailsToWriteToDB.containsKey(foundEmail)) {
                    // if it is, we skip it
                    continue;
                }

                //if we got here, it means we have a new email
                LocalDateTime timestamp = LocalDateTime.now();
                //create a new EmailRecord object
                EmailRecord emailRecord = new EmailRecord(foundEmail, currentURL, timestamp);
                //add it to our emails to write to DB map
                emailsToWriteToDB.put(foundEmail, emailRecord);

                //add to the unique emails set
                uniqueEmails.add(foundEmail);

                if (emailCount.get() >= TARGET_EMAILS) {
                    stopCrawling = true; //everyone stops
                }
            }

            //log it
            System.out.println("Found new email: " + foundEmail + " on page: " + currentURL +
                    "| Emails found: " + emailCount.incrementAndGet());

            // Check if we reached the target number of emails
            if (emailsToWriteToDB.size() >= EMAIL_BATCH) {
                //Write to db right away
                synchronized (dbLock) {
                    writeEmailsToDB();
                }
            }
        }

    }

    private void getAndQueueNewLinks(Document webPage//, String currentURL
    ) {
        Elements links = webPage.select("a[href]");

        for (Element link : links) {
            String linkURL = link.absUrl("href"); // Get the absolute URL

            synchronized (visitedURLs) {
                // add the link to the queue if it is not already visited or in our queue
                if (linkURL.startsWith("https://") && !visitedURLs.contains(linkURL) && !URLsToVisit.contains(linkURL)) {
                    URLsToVisit.offer(linkURL);
                }
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
        try {
            dbConnectionUrl = DriverManager.getConnection(connectionUrl);
            System.out.println("Connected to the database successfully.");

        } catch (SQLException e) {
            throw new RuntimeException("DB connection failed: " + e.getMessage());
        }
    }
}