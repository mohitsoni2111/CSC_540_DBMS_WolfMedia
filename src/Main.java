import java.sql.*;

public class Main {
    static final String USERNAME = "kiglaze";
    static final String PW = "000989070";
    static final String jdbcURL = "jdbc:mariadb://classdb2.csc.ncsu.edu:3306/kiglaze";
    // Put your oracle ID and password here

    private static Connection connection = null;
    private static Statement statement = null;
    private static final ResultSet result = null;

    /**
     * Must run with "initialize" command first! If you run in a 2nd time,
     * then it will reset the database.
     * @param args
     */
    public static void main(String[] args) {
        try {
            connectToDatabase();

            if(args.length > 0) {
                String argvCommand = args[0];
                try {
                    for (String argCommand : args) {
                        runFunctionBasedOnArgvCommand(argCommand);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("Hello world!");
            close();
        } catch (SQLException e) {
            System.out.println(e);
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            System.out.println(e);
            throw new RuntimeException(e);
        }
    }

    private static void runFunctionBasedOnArgvCommand(String commandText) throws SQLException {
        switch (commandText)
        {
            case "initialize":
                initialize();
                break;
            case "fullMonthlyPaymentsLifeCycle":
                fullMonthlyPaymentsLifeCycle();
                break;
            case "handleAllMonthlyPaymentsFromService":
                handleAllMonthlyPaymentsFromService();
                break;
            case "allUsersPayStreamingService":
                allUsersPayStreamingService();
                break;
            case "printServiceTotalRevenue":
                printServiceTotalRevenue();
                break;
            case "printServiceMonthlyRevenue":
                printServiceMonthlyRevenue();
                break;
            case "printServiceAnnualRevenue":
                printServiceAnnualRevenue();
                break;
            case "printAllServiceRevenueReports":
                printAllServiceRevenueReports();
                break;
            case "printRecordLabelTotalPayments":
                printRecordLabelTotalPayments();
                break;
            case "printRecordLabelMonthlyPayments":
                printRecordLabelMonthlyPayments();
                break;
            case "printRecordLabelAnnualPayments":
                printRecordLabelAnnualPayments();
                break;
            case "printPodcastHostTotalPayments":
                printPodcastHostTotalPayments();
                break;
            case "printPodcastHostMonthlyPayments":
                printPodcastHostMonthlyPayments();
                break;
            case "printPodcastHostAnnualPayments":
                printPodcastHostAnnualPayments();
                break;
            case "printArtistTotalPayments":
                printArtistTotalPayments();
                break;
            case "printArtistMonthlyPayments":
                printArtistMonthlyPayments();
                break;
            case "printArtistAnnualPayments":
                printArtistAnnualPayments();
                break;
            case "printAllFinancialReports":
                printAllFinancialReports();
                break;
            case "printSongsForArtist":
                printSongsForArtist("'ar2001'");
                //printSongsForArtist("ar2001' OR '1'='1");
                break;
            case "printSongsForAlbum":
                printSongsForAlbum("'al4001'");
                break;
            default:
                //code
        }
    }

    /**
     * All users pay the streaming service. The streaming service collects revenue.
     * @throws SQLException
     */
    private static void allUsersPayStreamingService() throws SQLException {
        statement.executeUpdate("INSERT INTO StreamingServiceMonthlyRevenue (monthYear, revenue) VALUES((DATE_FORMAT(NOW(), '%b %Y')), (SELECT SUM(monthlySubscriptionFee) FROM Users WHERE subscriptionIsActiveStatus IS TRUE)) ON DUPLICATE KEY UPDATE\n" +
                "    revenue=revenue+(SELECT SUM(monthlySubscriptionFee) FROM Users WHERE subscriptionIsActiveStatus IS TRUE);");
    }

    /**
     * Pays all podcast hosts for the current month.
     * Only need to run once per month.
     * @throws SQLException
     */
    private static void servicePaysAllPodcastHosts() throws SQLException {
        // TODO needs to be wrapped in a transaction. Verify that deduction (2nd executeUpdate) is correct.
        statement.executeUpdate("INSERT INTO Pays (earnerID, amount, monthYear)\n" +
                "SELECT * FROM (SELECT PodcastHosts.podcastHostEarnerId,\n" +
                "       SUM(PodcastHosts.flatFee + (PodcastHosts.adBonus * IFNULL(podcastEpisodeAdvertisementCount, 0))) AS owed,\n" +
                "       DATE_FORMAT(NOW(), '%b %Y')                                                                      AS monthYearEntry\n" +
                "FROM PodcastEpisodes\n" +
                "         LEFT JOIN Runs ON Runs.podcastId = PodcastEpisodes.podcastId\n" +
                "         LEFT JOIN PodcastHosts on Runs.podcastHostEarnerId = PodcastHosts.podcastHostEarnerId\n" +
                "         LEFT JOIN Earners on Earners.earnerID = PodcastHosts.podcastHostEarnerId\n" +
                "GROUP BY Runs.podcastHostEarnerId, monthYearEntry) as dt\n" +
                "ON DUPLICATE KEY UPDATE amount=amount+owed;");
        statement.executeUpdate("INSERT INTO StreamingServiceMonthlyRevenue (SELECT DATE_FORMAT(NOW(), '%b %Y'), (SELECT 0-SUM(amount) from Pays WHERE monthYear = DATE_FORMAT(NOW(), '%b %Y') AND earnerID IN (SELECT earnerID FROM PodcastHosts) GROUP BY monthYear)) ON DUPLICATE KEY UPDATE\n" +
                "    revenue = revenue - (SELECT SUM(amount) from Pays WHERE monthYear = DATE_FORMAT(NOW(), '%b %Y') AND earnerID IN (SELECT earnerID FROM PodcastHosts) GROUP BY monthYear);\n");
    }

    /**
     * Pays all record labels for the current month. Happens BEFORE record label pays the artists.
     * Only need to run once per month.
     * @throws SQLException
     */
    private static void servicePaysAllRecordLabels() throws SQLException {
        // TODO Wrap in Transaction.
        // Pays all record labels, without deducting from the revenue of the streaming service.
        statement.executeUpdate("INSERT INTO Pays (earnerID, amount, monthYear)\n" +
                "SELECT * FROM (SELECT RecordLabels.earnerID AS eID,\n" +
                "                      SUM(IFNULL(Songs.songRoyaltyRatePerPlay, 0)*IFNULL(Songs.playCountCurrentMonth, 0)) AS owed,\n" +
                "                      DATE_FORMAT(NOW(), '%b %Y') AS currentMonthYear\n" +
                "               FROM RecordLabels\n" +
                "                        LEFT JOIN Earners ON Earners.earnerID = RecordLabels.earnerID\n" +
                "                        LEFT JOIN Artists on RecordLabels.earnerID = Artists.recordLabelEarnerID\n" +
                "                        LEFT JOIN Sings on Artists.artistID = Sings.artistID\n" +
                "                        LEFT JOIN Songs ON Sings.songID = Songs.songID\n" +
                "               GROUP BY eID) as dt\n" +
                "ON DUPLICATE KEY UPDATE amount=amount+owed;");
        // Deduct payments from the streaming service.
        statement.executeUpdate("INSERT INTO StreamingServiceMonthlyRevenue (SELECT DATE_FORMAT(NOW(), '%b %Y'), (SELECT 0-SUM(amount) from Pays WHERE monthYear = DATE_FORMAT(NOW(), '%b %Y') AND earnerID IN (SELECT earnerID FROM RecordLabels) GROUP BY monthYear)) ON DUPLICATE KEY UPDATE\n" +
                "    revenue = revenue - (SELECT SUM(amount) from Pays WHERE monthYear = DATE_FORMAT(NOW(), '%b %Y') AND earnerID IN (SELECT earnerID FROM RecordLabels) GROUP BY monthYear);");
    }

    /**
     * Record Labels pay the Artists after they have been paid.
     * @throws SQLException
     */
    private static void recordLabelsPayArtists() throws SQLException {
        // TODO implement
    }

    /**
     * Handles all steps of the payments aside from the users paying the streaming service.
     * @throws SQLException
     */
    private static void handleAllMonthlyPaymentsFromService() throws SQLException {
        servicePaysAllPodcastHosts();
        servicePaysAllRecordLabels();
        recordLabelsPayArtists();
    }

    /**
     * Users pay streaming service, then streaming service pays out money to earners.
     * Then, record labels pay artists.
     * @throws SQLException
     */
    private static void fullMonthlyPaymentsLifeCycle() throws SQLException {
        allUsersPayStreamingService();
        handleAllMonthlyPaymentsFromService();
    }

    private static void printAllServiceRevenueReports() throws SQLException {
        printServiceMonthlyRevenue();
        printServiceAnnualRevenue();
        printServiceTotalRevenue();
        System.out.println();
    }

    private static void printServiceTotalRevenue() throws SQLException {
        System.out.println("Service total revenue:");
        ResultSet totalRevenue = statement.executeQuery("SELECT SUM(revenue) AS total FROM StreamingServiceMonthlyRevenue;");
        while (totalRevenue.next())
        {
            String total = totalRevenue.getString("total");
            // print the results
            System.out.format("%s\n", total);
        }
        System.out.println();
    }



    private static void printServiceMonthlyRevenue() throws SQLException {
        ResultSet totalRevenue = statement.executeQuery("SELECT * FROM StreamingServiceMonthlyRevenue;");
        System.out.println("Service monthly revenue:");
        while (totalRevenue.next())
        {
            String monthYear = totalRevenue.getString("monthYear");
            String revenue = totalRevenue.getString("revenue");
            // print the results
            System.out.format("%s: %s\n", monthYear, revenue);
        }
        System.out.println();
    }

    private static void printServiceAnnualRevenue() throws SQLException {
        ResultSet totalRevenue = statement.executeQuery("SELECT SUBSTR(monthYear, -4) AS year, SUM(revenue) AS total FROM StreamingServiceMonthlyRevenue\n" +
                "    GROUP BY year;");
        System.out.println("Service annual revenue:");
        while (totalRevenue.next())
        {
            String year = totalRevenue.getString("year");
            String revenue = totalRevenue.getString("total");
            // print the results
            System.out.format("%s: %s\n", year, revenue);
        }
        System.out.println();
    }

    private static void printRecordLabelTotalPayments() throws SQLException {
        System.out.println("Record Label total payments:");
        ResultSet totalRevenue = statement.executeQuery("SELECT earnerID, SUM(amount) AS total FROM Pays WHERE earnerID IN (SELECT earnerID FROM RecordLabels) GROUP BY earnerID;\n");
        while (totalRevenue.next())
        {
            String id = totalRevenue.getString("earnerID");
            String total = totalRevenue.getString("total");
            // print the results
            System.out.format("%s: %s\n", id, total);
        }
        System.out.println();
    }
    private static void printRecordLabelMonthlyPayments() throws SQLException {
        ResultSet totalRevenue = statement.executeQuery("SELECT earnerID, monthYear, SUM(amount) AS revenue FROM Pays WHERE earnerID IN (SELECT RecordLabels.earnerID FROM RecordLabels) GROUP BY earnerID, monthYear;\n");
        System.out.println("Record Labels monthly payments summary:");
        while (totalRevenue.next())
        {
            String id = totalRevenue.getString("earnerID");
            String monthYear = totalRevenue.getString("monthYear");
            String revenue = totalRevenue.getString("revenue");
            // print the results
            System.out.format("%s, %s: %s\n", id, monthYear, revenue);
        }
        System.out.println();
    }
    private static void printRecordLabelAnnualPayments() throws SQLException {
        ResultSet totalRevenue = statement.executeQuery("SELECT earnerID, SUBSTR(monthYear, -4) AS year, SUM(amount) AS revenue FROM Pays WHERE earnerID IN (SELECT RecordLabels.earnerID FROM RecordLabels) GROUP BY earnerID, year;\n");
        System.out.println("Record Labels annual payments summary:");
        while (totalRevenue.next())
        {
            String id = totalRevenue.getString("earnerID");
            String year = totalRevenue.getString("year");
            String revenue = totalRevenue.getString("revenue");
            // print the results
            System.out.format("%s, %s: %s\n", id, year, revenue);
        }
        System.out.println();
    }

    private static void printPodcastHostTotalPayments() throws SQLException {
        System.out.println("Podcast Hosts total payments:");
        ResultSet totalRevenue = statement.executeQuery("SELECT earnerID, SUM(amount) AS total FROM Pays WHERE earnerID IN (SELECT podcastHostEarnerId FROM PodcastHosts) GROUP BY earnerID;\n");
        while (totalRevenue.next())
        {
            String id = totalRevenue.getString("earnerID");
            String total = totalRevenue.getString("total");
            // print the results
            System.out.format("%s: %s\n", id, total);
        }
        System.out.println();
    }
    private static void printPodcastHostMonthlyPayments() throws SQLException {
        ResultSet totalRevenue = statement.executeQuery("SELECT earnerID, monthYear, SUM(amount) AS revenue FROM Pays WHERE earnerID IN (SELECT podcastHostEarnerId FROM PodcastHosts) GROUP BY earnerID, monthYear;\n");
        System.out.println("Podcast Hosts monthly payments summary:");
        while (totalRevenue.next())
        {
            String id = totalRevenue.getString("earnerID");
            String monthYear = totalRevenue.getString("monthYear");
            String revenue = totalRevenue.getString("revenue");
            // print the results
            System.out.format("%s, %s: %s\n", id, monthYear, revenue);
        }
        System.out.println();
    }
    private static void printPodcastHostAnnualPayments() throws SQLException {
        ResultSet totalRevenue = statement.executeQuery("SELECT earnerID, SUBSTR(monthYear, -4) AS year, SUM(amount) AS revenue FROM Pays WHERE earnerID IN (SELECT podcastHostEarnerId FROM PodcastHosts) GROUP BY earnerID, year;\n");
        System.out.println("Podcast Hosts annual payments summary:");
        while (totalRevenue.next())
        {
            String id = totalRevenue.getString("earnerID");
            String year = totalRevenue.getString("year");
            String revenue = totalRevenue.getString("revenue");
            // print the results
            System.out.format("%s, %s: %s\n", id, year, revenue);
        }
        System.out.println();
    }

    private static void printArtistTotalPayments() throws SQLException {
        //TODO
    }
    private static void printArtistMonthlyPayments() throws SQLException {
        //TODO
    }
    private static void printArtistAnnualPayments() throws SQLException {
        //TODO
    }

    private static void printSongsForArtist(String artistId) throws SQLException {
        String query = "SELECT Artists.artistID, Artists.artistName, Songs.* FROM Artists\n" +
                "    LEFT JOIN Sings on Artists.artistID = Sings.artistID\n" +
                "    LEFT JOIN Songs on Sings.songID = Songs.songID\n" +
                "    WHERE Sings.artistID = " + artistId;
        ResultSet rs = statement.executeQuery(query);
        System.out.format("Songs for artist with id: %s\n", artistId);
        while (rs.next())
        {
            String id = rs.getString("songID");
            String songTitle = rs.getString("songTitle");
            // print the results
            System.out.format("%s, %s\n", id, songTitle);
        }
        System.out.println();
    }
    private static void printSongsForAlbum(String albumId) throws SQLException {
        String query = "SELECT * FROM Songs\n" +
                "    LEFT JOIN Albums on Songs.albumID = Albums.albumID\n" +
                "    WHERE Albums.albumID = " + albumId;
        ResultSet rs = statement.executeQuery(query);
        System.out.format("Songs for album with id: %s\n", albumId);
        while (rs.next())
        {
            String id = rs.getString("songID");
            String songTitle = rs.getString("songTitle");
            // print the results
            System.out.format("%s, %s\n", id, songTitle);
        }
        System.out.println();
    }
    private static void printPodcastEpisodesForPodcast(String podcastId) throws SQLException {
        //TODO
    }
    private static void printPodcastEpisodesForPodcastHost() throws SQLException {
        //TODO
    }
    private static void printMonthlyPlayCountsForSongs() throws SQLException {
        //TODO
    }
    private static void printMonthlyPlayCountsForArtists() throws SQLException {
        //TODO
    }
    private static void printMonthlyPlayCountsForAlbums() throws SQLException {
        //TODO
    }
    private static void printAllFinancialReports() throws SQLException {
        printServiceTotalRevenue();
        printServiceMonthlyRevenue();
        printServiceAnnualRevenue();
        printRecordLabelTotalPayments();
        printRecordLabelMonthlyPayments();
        printRecordLabelAnnualPayments();
        printPodcastHostTotalPayments();
        printPodcastHostMonthlyPayments();
        printPodcastHostAnnualPayments();
        printArtistTotalPayments();
        printArtistMonthlyPayments();
        printArtistAnnualPayments();
    }


    private static void createTables() {
        try {
            statement.executeUpdate("CREATE TABLE Users\n" +
                    "(\n" +
                    "    userID                     VARCHAR(255)  NOT NULL,\n" +
                    "    userFirstName              VARCHAR(255)  NOT NULL,\n" +
                    "    userLastName               VARCHAR(255)  NOT NULL,\n" +
                    "    subscriptionIsActiveStatus BOOLEAN       NOT NULL,\n" +
                    "    monthlySubscriptionFee     SMALLINT      NOT NULL,\n" +
                    "    userEmail                  NVARCHAR(255) NOT NULL,\n" +
                    "    registrationDate           DATE          NOT NULL,\n" +
                    "    PRIMARY KEY (userID)\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE StreamingServiceMonthlyRevenue(\n" +
                    "    monthYear     VARCHAR(255)    NOT NULL,\n" +
                    "    revenue         DOUBLE(9, 2)   NOT NULL,\n" +
                    "    PRIMARY KEY (monthYear)\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE Earners(\n" +
                    "    earnerID VARCHAR(255) NOT NULL PRIMARY KEY\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE RecordLabels(\n" +
                    "    earnerID        VARCHAR(255)    NOT NULL,\n" +
                    "    recordLabelName VARCHAR(255)    NOT NULL,\n" +
                    "    PRIMARY KEY (earnerID),\n" +
                    "    FOREIGN KEY (earnerID) REFERENCES Earners (earnerID)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE PodcastHosts\n" +
                    "(\n" +
                    "    podcastHostEarnerId  varchar(255) NOT NULL,\n" +
                    "    podcastHostFirstName varchar(255) NOT NULL,\n" +
                    "    podcastHostLastName  varchar(255) NOT NULL,\n" +
                    "    podcastHostEmail     varchar(255) NOT NULL,\n" +
                    "    podcastHostPhone     char(10),\n" +
                    "    podcastHostCity      varchar(50),\n" +
                    "    flatFee              DOUBLE(9, 2) NOT NULL,\n" +
                    "    adBonus              DOUBLE(9, 2) NOT NULL,\n" +
                    "    PRIMARY KEY (podcastHostEarnerId),\n" +
                    "    FOREIGN KEY (podcastHostEarnerId) REFERENCES Earners (earnerID)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE SongGenres(\n" +
                    "    songGenreName VARCHAR(255) NOT NULL PRIMARY KEY\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE Artists\n" +
                    "(\n" +
                    "    artistID               VARCHAR(255) NOT NULL,\n" +
                    "    recordLabelEarnerID    VARCHAR(255) NOT NULL,\n" +
                    "    artistName             VARCHAR(255) NOT NULL,\n" +
                    "    artistStatusIsActive   BOOLEAN      NOT NULL,\n" +
                    "    artistMonthlyListeners INT          NOT NULL,\n" +
                    "    artistPrimaryGenre     VARCHAR(255),\n" +
                    "    artistType             VARCHAR(255),\n" +
                    "    artistCountry          VARCHAR(255),\n" +
                    "    PRIMARY KEY (artistID),\n" +
                    "    FOREIGN KEY (recordLabelEarnerID) REFERENCES RecordLabels (earnerID)\n" +
                    "        ON UPDATE CASCADE,\n" +
                    "    FOREIGN KEY (artistPrimaryGenre) REFERENCES SongGenres (songGenreName)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE Albums(\n" +
                    "    albumID           VARCHAR(255) NOT NULL,\n" +
                    "    albumName         VARCHAR(255) NOT NULL,\n" +
                    "    albumEdition      VARCHAR(255),\n" +
                    "    albumTrackNumbers INT          NOT NULL,\n" +
                    "    albumReleaseYear  YEAR         NOT NULL,\n" +
                    "    PRIMARY KEY (albumID)\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE Songs(\n" +
                    "    songID            VARCHAR(255)      NOT NULL,\n" +
                    "    songTitle         VARCHAR(255)      NOT NULL,\n" +
                    "    albumID             VARCHAR(255)      NOT NULL,\n" +
                    "    playCountCurrentMonth     INT               NOT NULL,\n" +
                    "    songRoyaltyRatePerPlay   DOUBLE(9,2)       NOT NULL,\n" +
                    "    isSongRoyaltyPaid BOOLEAN      NOT NULL,\n" +
                    "    songReleaseDate   DATE              NOT NULL,\n" +
                    "    songLanguage      VARCHAR(255),\n" +
                    "    songDuration      TIME,\n" +
                    "    PRIMARY KEY (songID),\n" +
                    "    FOREIGN KEY (albumID) REFERENCES Albums (albumID)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE Sings(\n" +
                    "                      artistID        VARCHAR(255)    NOT NULL,\n" +
                    "                      songID          VARCHAR(255)    NOT NULL,\n" +
                    "                      PRIMARY KEY (artistId, songID),\n" +
                    "                      FOREIGN KEY (songID)\n" +
                    "                          REFERENCES Songs (songID)\n" +
                    "                          ON UPDATE CASCADE,\n" +
                    "                      FOREIGN KEY (artistId) REFERENCES Artists (artistId)\n" +
                    "                          ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE Collaborates(\n" +
                    "     artistIDMain            VARCHAR(255) NOT NULL,\n" +
                    "     artistIDCollaborated    VARCHAR(255) NOT NULL,\n" +
                    "     songID                  VARCHAR(255) NOT NULL,\n" +
                    "     PRIMARY KEY (artistIDMain, artistIDCollaborated, songID),\n" +
                    "     FOREIGN KEY (artistIDMain)\n" +
                    "         REFERENCES Artists (artistID)\n" +
                    "         ON UPDATE CASCADE,\n" +
                    "     FOREIGN KEY (artistIDCollaborated)\n" +
                    "         REFERENCES Artists (artistID)\n" +
                    "         ON UPDATE CASCADE,\n" +
                    "     FOREIGN KEY (songID)\n" +
                    "         REFERENCES Songs (songID)\n" +
                    "         ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE SongsLogs\n" +
                    "(\n" +
                    "    songId           VARCHAR(255) NOT NULL,\n" +
                    "    playCount        INT          NOT NULL,\n" +
                    "    songLogMonthYear VARCHAR(255) NOT NULL,\n" +
                    "    PRIMARY KEY (songId, songLogMonthYear)\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE SongBelongsTo(\n" +
                    "    songID          VARCHAR(255) NOT NULL,\n" +
                    "    songGenreName   VARCHAR(255) NOT NULL,\n" +
                    "    PRIMARY KEY (songID, songGenreName),\n" +
                    "    FOREIGN KEY  (songID)\n" +
                    "        REFERENCES Songs (songID)\n" +
                    "        ON UPDATE CASCADE,\n" +
                    "    FOREIGN KEY (songGenreName)\n" +
                    "        REFERENCES SongGenres (songGenreName)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE Pays\n" +
                    "(\n" +
                    "    earnerID  VARCHAR(255) NOT NULL,\n" +
                    "    amount    DOUBLE(9, 2) NOT NULL,\n" +
                    "    monthYear VARCHAR(255) NOT NULL,\n" +
                    "    PRIMARY KEY (earnerID, monthYear),\n" +
                    "    FOREIGN KEY (earnerID)\n" +
                    "        REFERENCES Earners (earnerID)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE PaysArtists\n" +
                    "(\n" +
                    "    artistId  VARCHAR(255) NOT NULL,\n" +
                    "    amount    DOUBLE(9, 2) NOT NULL,\n" +
                    "    monthYear VARCHAR(255) NOT NULL,\n" +
                    "    PRIMARY KEY (artistId, monthYear),\n" +
                    "    FOREIGN KEY (artistId)\n" +
                    "        REFERENCES Artists (artistID)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE Podcasts\n" +
                    "(\n" +
                    "    podcastId               varchar(255) NOT NULL,\n" +
                    "    podcastName             varchar(255) NOT NULL,\n" +
                    "    podcastEpisodeCount     int,\n" +
                    "    flatFeePerEpisode       DOUBLE(9, 2) NOT NULL,\n" +
                    "    podcastRating           DECIMAL(2, 1),\n" +
                    "    podcastTotalSubscribers int,\n" +
                    "    podcastLanguage         varchar(255),\n" +
                    "    podcastCountry          varchar(255),\n" +
                    "    PRIMARY KEY (podcastId)\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE PodcastEpisodes\n" +
                    "(\n" +
                    "    podcastEpisodeId                 varchar(255) NOT NULL,\n" +
                    "    podcastEpisodeTitle              varchar(255) NOT NULL,\n" +
                    "    podcastId                        varchar(255) NOT NULL,\n" +
                    "    podcastEpisodeListeningCount     int,\n" +
                    "    podcastEpisodeAdvertisementCount int,\n" +
                    "    podcastEpisodeDuration           TIME,\n" +
                    "    podcastEpisodeReleaseDate        DATE,\n" +
                    "    FOREIGN KEY (podcastId) REFERENCES Podcasts (podcastId),\n" +
                    "    PRIMARY KEY (podcastEpisodeId)\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE PodcastSponsors(\n" +
                    "    podcastSponsorName varchar(255) PRIMARY KEY\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE PodcastSponsoredBy\n" +
                    "(\n" +
                    "    podcastSponsorName varchar(255),\n" +
                    "    podcastId          varchar(255),\n" +
                    "    PRIMARY KEY (podcastSponsorName, podcastId),\n" +
                    "    FOREIGN KEY (podcastSponsorName)\n" +
                    "        REFERENCES PodcastSponsors (podcastSponsorName)\n" +
                    "        ON UPDATE CASCADE,\n" +
                    "    FOREIGN KEY (podcastId)\n" +
                    "        REFERENCES Podcasts (podcastId)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE PodcastGenres(\n" +
                    "    podcastGenreName varchar(255) PRIMARY KEY\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE SpecialGuests\n" +
                    "(\n" +
                    "    guestName varchar(255) NOT NULL,\n" +
                    "    PRIMARY KEY (guestName)\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE Runs\n" +
                    "(\n" +
                    "    podcastHostEarnerId varchar(255) NOT NULL,\n" +
                    "    podcastId           varchar(255) NOT NULL,\n" +
                    "    PRIMARY KEY (podcastHostEarnerId, podcastId),\n" +
                    "    FOREIGN KEY (podcastHostEarnerId)\n" +
                    "        REFERENCES PodcastHosts (podcastHostEarnerId)\n" +
                    "        ON UPDATE CASCADE,\n" +
                    "    FOREIGN KEY (podcastId)\n" +
                    "        REFERENCES Podcasts (podcastId)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE PodcastBelongsTo\n" +
                    "(\n" +
                    "    podcastGenreName varchar(255),\n" +
                    "    podcastId      varchar(255),\n" +
                    "    PRIMARY KEY (podcastGenreName, podcastId),\n" +
                    "    FOREIGN KEY (podcastGenreName)\n" +
                    "        REFERENCES PodcastGenres (podcastGenreName)\n" +
                    "        ON UPDATE CASCADE,\n" +
                    "    FOREIGN KEY (podcastId)\n" +
                    "        REFERENCES Podcasts (podcastId)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");
            statement.executeUpdate("CREATE TABLE Features\n" +
                    "(\n" +
                    "    guestName             varchar(255)          NOT NULL,\n" +
                    "    podcastEpisodeId         varchar(255) NOT NULL,\n" +
                    "    PRIMARY KEY (guestName, podcastEpisodeId),\n" +
                    "    FOREIGN KEY (guestName)\n" +
                    "        REFERENCES SpecialGuests (guestName)\n" +
                    "        ON UPDATE CASCADE,\n" +
                    "    FOREIGN KEY (podcastEpisodeId)\n" +
                    "        REFERENCES PodcastEpisodes (podcastEpisodeId)\n" +
                    "        ON UPDATE CASCADE\n" +
                    ");");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertRows() {
        try {
            statement.executeUpdate("INSERT INTO Users (userID, userFirstName, userLastName, subscriptionIsActiveStatus, monthlySubscriptionFee, userEmail, registrationDate)\n" +
                    "VALUES\n" +
                    "    ('u8001', 'Alex', 'A', true, 10, 'alex.a@ncsu.edu', NOW()),\n" +
                    "    ('u8002', 'John', 'J', true, 10, 'john.j@ncsu.edu', NOW());");
            statement.executeUpdate("INSERT INTO StreamingServiceMonthlyRevenue(monthYear, revenue)\n" +
                    "VALUES\n" +
                    "    ('Jan 2023', 1111),\n" +
                    "    ('Feb 2023', 2222),\n" +
                    "    ('Mar 2023', 3333),\n" +
                    "    ('Apr 2023', 123000);");
            statement.executeUpdate("INSERT INTO Earners (earnerID)\n" +
                    "    VALUES ('rl3001'), ('rl3002'), ('ph6001');");
            statement.executeUpdate("INSERT INTO RecordLabels (earnerID, recordLabelName)\n" +
                    "    VALUES\n" +
                    "        ('rl3001', 'Elevate Records'),\n" +
                    "        ('rl3002', 'Melodic Avenue Music');");
            statement.executeUpdate("INSERT INTO PodcastHosts (podcastHostEarnerId, podcastHostFirstName, podcastHostLastName, podcastHostEmail, podcastHostPhone, podcastHostCity, flatFee, adBonus)\n" +
                    "VALUES\n" +
                    "    ('ph6001', 'Matthew', 'Wilson', 'mwilson@gmail.com', '9195154000', 'San Diego', 5000, 200);");
            statement.executeUpdate("INSERT INTO SongGenres (songGenreName)\n" +
                    "    VALUES ('Pop'), ('Rock'), ('Classical'), ('Jazz'), ('Country');");
            statement.executeUpdate("INSERT INTO Artists (artistID, recordLabelEarnerID, artistName, artistStatusIsActive,\n" +
                    "                     artistMonthlyListeners, artistPrimaryGenre, artistType, artistCountry)\n" +
                    "    VALUES\n" +
                    "        ('ar2001', 'rl3001', 'Forest', true, 25, 'Pop', 'band', 'US'),\n" +
                    "        ('ar2002', 'rl3002', 'Rain', true, 55, 'Rock', 'musician', 'US');");
            statement.executeUpdate("INSERT INTO Albums(albumID, albumName, albumEdition, albumTrackNumbers, albumReleaseYear)\n" +
                    "    VALUES\n" +
                    "        ('al4001', 'Electric Oasis', '1st', 2, 2008),\n" +
                    "        ('al4002', 'Lost in the Echoes', '2nd', 2, 2009);");
            statement.executeUpdate("INSERT INTO Songs(songID, songTitle, albumID, playCountCurrentMonth, songRoyaltyRatePerPlay, isSongRoyaltyPaid, songReleaseDate, songLanguage,\n" +
                    "                  songDuration)\n" +
                    "    VALUES\n" +
                    "        ('s1001', 'Electric Dreamscape', 'al4001', 500, 0.10, false, '2000-12-12', 'English', '0:3:30'),\n" +
                    "        ('s1002', 'Midnight Mirage', 'al4001', 1000, 0.10, false, '2001-12-12', 'English', '0:3:30'),\n" +
                    "        ('s1003', 'Echoes of You', 'al4002', 100, 0.10, false, '2002-12-12', 'English', '0:3:30'),\n" +
                    "        ('s1004', 'Rainy Nights', 'al4002', 200, 0.10, false, '2003-12-12', 'English', '0:3:30');\n");
            statement.executeUpdate("INSERT INTO Sings(artistID, songID)\n" +
                    "VALUES\n" +
                    "    ('ar2001', 's1001'),\n" +
                    "    ('ar2001', 's1002'),\n" +
                    "    ('ar2002', 's1003'),\n" +
                    "    ('ar2002', 's1004');");
            statement.executeUpdate("INSERT INTO Collaborates\n" +
                    "VALUES\n" +
                    "    ('ar2001', 'ar2002', 's1002');");
            statement.executeUpdate("INSERT INTO SongsLogs(songId, playCount, songLogMonthYear)\n" +
                    "    VALUES\n" +
                    "        ('s1001', 10, 'Jan 2023'),\n" +
                    "        ('s1001', 20, 'Feb 2023'),\n" +
                    "        ('s1001', 30, 'Mar 2023'),\n" +
                    "        ('s1002', 100, 'Jan 2023'),\n" +
                    "        ('s1002', 200, 'Feb 2023'),\n" +
                    "        ('s1002', 300, 'Mar 2023'),\n" +
                    "        ('s1003', 1000, 'Jan 2023'),\n" +
                    "        ('s1003', 2000, 'Feb 2023'),\n" +
                    "        ('s1003', 3000, 'Mar 2023'),\n" +
                    "        ('s1004', 10000, 'Jan 2023'),\n" +
                    "        ('s1004', 20000, 'Feb 2023'),\n" +
                    "        ('s1004', 30000, 'Mar 2023');");
            statement.executeUpdate("INSERT INTO SongBelongsTo(songID, songGenreName)\n" +
                    "    VALUES\n" +
                    "        ('s1001', 'Classical'),\n" +
                    "        ('s1002', 'Rock'),\n" +
                    "        ('s1003', 'Pop'),\n" +
                    "        ('s1004', 'Classical');");
            statement.executeUpdate("INSERT INTO Pays(earnerID, amount, monthYear)\n" +
                    "    VALUES\n" +
                    "        ('ph6001', 20, 'Jan 2023'),\n" +
                    "        ('ph6001', 30, 'Feb 2023'),\n" +
                    "        ('ph6001', 40, 'Mar 2023'),\n" +
                    "        ('rl3001', 3.3, 'Jan 2023'),\n" +
                    "        ('rl3001', 6.6, 'Feb 2023'),\n" +
                    "        ('rl3001', 9.9, 'Mar 2023'),\n" +
                    "        ('rl3002', 330, 'Jan 2023'),\n" +
                    "        ('rl3002', 660, 'Feb 2023'),\n" +
                    "        ('rl3002', 990, 'Mar 2023');");
            statement.executeUpdate("INSERT INTO PaysArtists(artistId, amount, monthYear)\n" +
                    "VALUES\n" +
                    "    ('ar2001', 4.2, 'Jan 2023'),\n" +
                    "    ('ar2001', 8.4, 'Feb 2023'),\n" +
                    "    ('ar2001', 12.6, 'Mar 2023'),\n" +
                    "    ('ar2002', 703.5, 'Jan 2023'),\n" +
                    "    ('ar2002', 1547, 'Feb 2023'),\n" +
                    "    ('ar2002', 2320.5, 'Mar 2023');");
            statement.executeUpdate("INSERT INTO Podcasts(podcastId, podcastName, podcastEpisodeCount, flatFeePerEpisode, podcastRating, podcastTotalSubscribers, podcastLanguage, podcastCountry)\n" +
                    "    VALUES\n" +
                    "        ('p5001','Mind Over Matter: Exploring the Power of the Human Mind', 5, 10, 4.5, 10, 'English', 'United States');");
            statement.executeUpdate("INSERT INTO PodcastEpisodes(podcastEpisodeId, podcastEpisodeTitle, podcastId, podcastEpisodeListeningCount, podcastEpisodeAdvertisementCount, podcastEpisodeDuration, podcastEpisodeReleaseDate)\n" +
                    "    VALUES\n" +
                    "        ('pe7001', 'The Science of Mindfulness', 'p5001', 100, null, '1:22:15', '2018-01-01'),\n" +
                    "        ('pe7002', 'Unlocking Your Potential', 'p5001', 200, null, '1:20:30', '2018-02-01');");
            statement.executeUpdate("INSERT INTO PodcastSponsors(podcastSponsorName)\n" +
                    "    VALUES\n" +
                    "        ('ExpressVPN'),\n" +
                    "        ('ZipRecruiter'),\n" +
                    "        ('Audible'),\n" +
                    "        ('DoorDash'),\n" +
                    "        ('Apple'),\n" +
                    "        ('IBM'),\n" +
                    "        ('CapitalOne'),\n" +
                    "        ('BetterHelp'),\n" +
                    "        ('Comcast');");
            statement.executeUpdate("INSERT INTO PodcastSponsoredBy(podcastSponsorName, podcastId)\n" +
                    "    VALUES\n" +
                    "        ('DoorDash', 'p5001'),\n" +
                    "        ('Audible', 'p5001'),\n" +
                    "        ('ExpressVPN', 'p5001');");
            statement.executeUpdate("INSERT INTO PodcastGenres(podcastGenreName)\n" +
                    "    VALUES\n" +
                    "        ('Crime'),\n" +
                    "        ('Comedy'),\n" +
                    "        ('Business'),\n" +
                    "        ('Kids'),\n" +
                    "        ('Sports'),\n" +
                    "        ('News'),\n" +
                    "        ('Interview'),\n" +
                    "        ('History'),\n" +
                    "        ('Politics');");
            statement.executeUpdate("INSERT INTO SpecialGuests(guestName)\n" +
                    "    VALUES\n" +
                    "        ('James Bond'),\n" +
                    "        ('Tony Stark'),\n" +
                    "        ('Elon Musk'),\n" +
                    "        ('Tim Cook'),\n" +
                    "        ('Penelope Cruz'),\n" +
                    "        ('Shakira');");
            statement.executeUpdate("INSERT INTO Runs(podcastHostEarnerId, podcastId)\n" +
                    "    VALUES\n" +
                    "        ('ph6001', 'p5001');");
            statement.executeUpdate("INSERT INTO PodcastBelongsTo(podcastGenreName, podcastId)\n" +
                    "    VALUES\n" +
                    "        ('Interview', 'p5001');");
            statement.executeUpdate("INSERT INTO Features(guestName, podcastEpisodeId)\n" +
                    "    VALUES\n" +
                    "        ('James Bond', 'pe7001'),\n" +
                    "        ('Penelope Cruz', 'pe7001'),\n" +
                    "        ('Tony Stark', 'pe7002');");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private static void initialize() {
        try {
            dropAllTables();
            createTables();
            insertRows();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (result != null) {
            try {
                result.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static void dropAllTables() {
        try {
            statement.executeUpdate("DROP TABLE IF EXISTS Features");
            statement.executeUpdate("DROP TABLE IF EXISTS PodcastBelongsTo");
            statement.executeUpdate("DROP TABLE IF EXISTS Runs");
            statement.executeUpdate("DROP TABLE IF EXISTS SpecialGuests");
            statement.executeUpdate("DROP TABLE IF EXISTS PodcastGenres");
            statement.executeUpdate("DROP TABLE IF EXISTS PodcastSponsoredBy");
            statement.executeUpdate("DROP TABLE IF EXISTS PodcastSponsors");
            statement.executeUpdate("DROP TABLE IF EXISTS PodcastEpisodes");
            statement.executeUpdate("DROP TABLE IF EXISTS Podcasts");
            statement.executeUpdate("DROP TABLE IF EXISTS PaysArtists");
            statement.executeUpdate("DROP TABLE IF EXISTS Pays");
            statement.executeUpdate("DROP TABLE IF EXISTS SongBelongsTo");
            statement.executeUpdate("DROP TABLE IF EXISTS SongsLogs");
            statement.executeUpdate("DROP TABLE IF EXISTS Collaborates");
            statement.executeUpdate("DROP TABLE IF EXISTS Sings");
            statement.executeUpdate("DROP TABLE IF EXISTS Songs");
            statement.executeUpdate("DROP TABLE IF EXISTS Albums");
            statement.executeUpdate("DROP TABLE IF EXISTS Artists");
            statement.executeUpdate("DROP TABLE IF EXISTS SongGenres");
            statement.executeUpdate("DROP TABLE IF EXISTS PodcastHosts");
            statement.executeUpdate("DROP TABLE IF EXISTS RecordLabels");
            statement.executeUpdate("DROP TABLE IF EXISTS Earners");
            statement.executeUpdate("DROP TABLE IF EXISTS StreamingServiceMonthlyRevenue");
            statement.executeUpdate("DROP TABLE IF EXISTS Users");
            //} catch (SQLException e) {
        } catch (Exception ignored) {
        }
    }

    private static void connectToDatabase() throws ClassNotFoundException, SQLException {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            connection = DriverManager.getConnection(jdbcURL, USERNAME, PW);
            statement = connection.createStatement();
        } catch (SQLException e) {
            throw new SQLException("Count not connect to database.");
        } catch (Exception e) {
            throw new ClassNotFoundException("Class for driver not found.");
        }
    }
}
