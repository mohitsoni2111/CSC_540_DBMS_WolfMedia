show databases;
use msoni;
show tables;

/*CREATE TABLE StreamingService(
    serviceName    VARCHAR(128) NOT NULL,
    serviceBalance DOUBLE(9, 2) NOT NULL,
    PRIMARY KEY (serviceName)
);
INSERT INTO StreamingService (serviceName, serviceBalance)
    VALUES ('Spotify', 0);
SELECT * FROM StreamingService;*/


-- --------------------------------------------------------------------------------


CREATE TABLE Users(
                      userID                     VARCHAR(255)  NOT NULL,
                      userFirstName              VARCHAR(255)  NOT NULL,
                      userLastName               VARCHAR(255)  NOT NULL,
                      subscriptionIsActiveStatus BOOLEAN       NOT NULL,
                      monthlySubscriptionFee     SMALLINT      NOT NULL,
                      userEmail                  NVARCHAR(255) NOT NULL,
                      registrationDate           DATE          NOT NULL,
                      PRIMARY KEY (userID)
);
INSERT INTO Users (userID, userFirstName, userLastName, subscriptionIsActiveStatus, monthlySubscriptionFee, userEmail,
                   registrationDate)
VALUES
    ('u8001', 'Alex', 'A', true, 10, 'alex.a@ncsu.edu', NOW()),
    ('u8002', 'John', 'J', true, 10, 'john.j@ncsu.edu', NOW());
SELECT * FROM Users;


-- --------------------------------------------------------------------------------

CREATE TABLE StreamingServiceMonthlyRevenue(
                                               monthYear     VARCHAR(255)    NOT NULL,
                                               revenue         DOUBLE(9, 2)   NOT NULL,
    PRIMARY KEY (monthYear)
);
INSERT INTO StreamingServiceMonthlyRevenue(monthYear, revenue)
VALUES
    ('Jan 2023', 1111),
    ('Feb 2023', 2222),
    ('Mar 2023', 3333),
    ('Apr 2023', 123000);
SELECT * FROM StreamingServiceMonthlyRevenue;
-- SELECT * FROM StreamingServiceMonthlyRevenue WHERE monthYear = DATE_FORMAT('2023-04-00 00:00:00', '%M %Y');
-- SELECT * FROM StreamingServiceMonthlyRevenue WHERE monthYear = DATE_FORMAT(NOW(), '%M %Y');

-- --------------------------------------------------------------------------------

CREATE TABLE Earners(
    earnerID VARCHAR(255) NOT NULL PRIMARY KEY
);
INSERT INTO Earners (earnerID)
VALUES ('rl3001'), ('rl3002'), ('ph6001');
SELECT * FROM Earners;

-- --------------------------------------------------------------------------------

CREATE TABLE RecordLabels(
                             earnerID        VARCHAR(255)    NOT NULL,
                             recordLabelName VARCHAR(255)    NOT NULL,
                             PRIMARY KEY (earnerID),
                             FOREIGN KEY (earnerID) REFERENCES Earners (earnerID)
                                 ON UPDATE CASCADE
);
INSERT INTO RecordLabels (earnerID, recordLabelName)
VALUES
    ('rl3001', 'Elevate Records'),
    ('rl3002', 'Melodic Avenue Music');
SELECT * FROM RecordLabels;

-- --------------------------------------------------------------------------------

CREATE TABLE PodcastHosts(
                             podcastHostEarnerId  varchar(255) NOT NULL,
                             podcastHostFirstName varchar(255) NOT NULL,
                             podcastHostLastName  varchar(255) NOT NULL,
                             podcastHostEmail     varchar(255) NOT NULL,
                             podcastHostPhone     char(10),
                             podcastHostCity      varchar(50),
                             flatFee              DOUBLE(9, 2) NOT NULL,
    adBonus              DOUBLE(9, 2) NOT NULL,
    PRIMARY KEY (podcastHostEarnerId),
    FOREIGN KEY (podcastHostEarnerId) REFERENCES Earners (earnerID)
        ON UPDATE CASCADE
);
INSERT INTO PodcastHosts (podcastHostEarnerId, podcastHostFirstName, podcastHostLastName, podcastHostEmail,
                          podcastHostPhone, podcastHostCity, flatFee, adBonus)
VALUES
    ('ph6001', 'Matthew', 'Wilson', 'mwilson@gmail.com', '9195154000', 'San Diego', 5000, 200);
SELECT * FROM PodcastHosts;

-- --------------------------------------------------------------------------------

CREATE TABLE SongGenres(
    songGenreName VARCHAR(255) NOT NULL PRIMARY KEY
);
INSERT INTO SongGenres (songGenreName)
VALUES ('Pop'), ('Rock'), ('Classical'), ('Jazz'), ('Country');
SELECT * FROM SongGenres;

-- --------------------------------------------------------------------------------

CREATE TABLE Artists(
                        artistID               VARCHAR(255) NOT NULL,
                        recordLabelEarnerID    VARCHAR(255) NOT NULL,
                        artistName             VARCHAR(255) NOT NULL,
                        artistStatusIsActive   BOOLEAN      NOT NULL,
                        artistMonthlyListeners INT          NOT NULL,
                        artistPrimaryGenre     VARCHAR(255),
                        artistType             VARCHAR(255),
                        artistCountry          VARCHAR(255),
                        PRIMARY KEY (artistID),
                        FOREIGN KEY (recordLabelEarnerID) REFERENCES RecordLabels (earnerID)
                            ON UPDATE CASCADE,
                        FOREIGN KEY (artistPrimaryGenre) REFERENCES SongGenres (songGenreName)
                            ON UPDATE CASCADE
);
INSERT INTO Artists (artistID, recordLabelEarnerID, artistName, artistStatusIsActive,
                     artistMonthlyListeners, artistPrimaryGenre, artistType, artistCountry)
VALUES
    ('ar2001', 'rl3001', 'Forest', true, 25, 'Pop', 'band', 'US'),
    ('ar2002', 'rl3002', 'Rain', true, 55, 'Rock', 'musician', 'US');
SELECT * FROM Artists;

-- --------------------------------------------------------------------------------

CREATE TABLE Albums(
                       albumID           VARCHAR(255) NOT NULL,
                       albumName         VARCHAR(255) NOT NULL,
                       albumEdition      VARCHAR(255),
                       albumTrackNumbers INT          NOT NULL,
                       albumReleaseYear  YEAR         NOT NULL,
                       PRIMARY KEY (albumID)
);
INSERT INTO Albums(albumID, albumName, albumEdition, albumTrackNumbers, albumReleaseYear)
VALUES
    ('al4001', 'Electric Oasis', '1st', 2, 2008),
    ('al4002', 'Lost in the Echoes', '2nd', 2, 2009);
SELECT * FROM Albums;

-- --------------------------------------------------------------------------------

CREATE TABLE Songs(
                      songID            VARCHAR(255)      NOT NULL,
                      songTitle         VARCHAR(255)      NOT NULL,
                      albumID             VARCHAR(255)      NOT NULL,
                      playCountCurrentMonth     INT               NOT NULL,
                      songRoyaltyRatePerPlay   DOUBLE(9,2)       NOT NULL,
    isSongRoyaltyPaid BOOLEAN      NOT NULL,
    songReleaseDate   DATE              NOT NULL,
    songLanguage      VARCHAR(255),
    songDuration      TIME,
    PRIMARY KEY (songID),
    FOREIGN KEY (albumID) REFERENCES Albums (albumID)
        ON UPDATE CASCADE
);
INSERT INTO Songs(songID, songTitle, albumID, playCountCurrentMonth, songRoyaltyRatePerPlay, isSongRoyaltyPaid,
                  songReleaseDate, songLanguage,
                  songDuration)
VALUES
    ('s1001', 'Electric Dreamscape', 'al4001', 500, 0.10, false, '2000-12-12', 'English', '0:3:30'),
    ('s1002', 'Midnight Mirage', 'al4001', 1000, 0.10, false, '2001-12-12', 'English', '0:3:30'),
    ('s1003', 'Echoes of You', 'al4002', 100, 0.10, false, '2002-12-12', 'English', '0:3:30'),
    ('s1004', 'Rainy Nights', 'al4002', 200, 0.10, false, '2003-12-12', 'English', '0:3:30');
SELECT * FROM Songs;

-- --------------------------------------------------------------------------------

CREATE TABLE Sings(
                      artistID        VARCHAR(255)    NOT NULL,
                      songID          VARCHAR(255)    NOT NULL,
                      PRIMARY KEY (artistId, songID),
                      FOREIGN KEY (songID)
                          REFERENCES Songs (songID)
                          ON UPDATE CASCADE,
                      FOREIGN KEY (artistId) REFERENCES Artists (artistId)
                          ON UPDATE CASCADE
);
INSERT INTO Sings(artistID, songID)
VALUES
    ('ar2001', 's1001'),
    ('ar2001', 's1002'),
    ('ar2002', 's1003'),
    ('ar2002', 's1004');
SELECT * FROM Sings;

-- --------------------------------------------------------------------------------

# Mention of song in collaboration
CREATE TABLE Collaborates(
                             artistIDMain            VARCHAR(255) NOT NULL,
                             artistIDCollaborated    VARCHAR(255) NOT NULL,
                             songID                  VARCHAR(255) NOT NULL,
                             PRIMARY KEY (artistIDMain, artistIDCollaborated, songID),
                             FOREIGN KEY (artistIDMain)
                                 REFERENCES Artists (artistID)
                                 ON UPDATE CASCADE,
                             FOREIGN KEY (artistIDCollaborated)
                                 REFERENCES Artists (artistID)
                                 ON UPDATE CASCADE,
                             FOREIGN KEY (songID)
                                 REFERENCES Songs (songID)
                                 ON UPDATE CASCADE
);
INSERT INTO Collaborates
VALUES
    ('ar2001', 'ar2002', 's1002');
SELECT * FROM Collaborates;

-- --------------------------------------------------------------------------------

CREATE TABLE SongsLogs(
                          songId           VARCHAR(255) NOT NULL,
                          playCount        INT          NOT NULL,
                          songLogMonthYear VARCHAR(255) NOT NULL,
                          PRIMARY KEY (songId, songLogMonthYear)
);
INSERT INTO SongsLogs(songId, playCount, songLogMonthYear)
VALUES
    ('s1001', 10, 'Jan 2023'),
    ('s1001', 20, 'Feb 2023'),
    ('s1001', 30, 'Mar 2023'),
    ('s1002', 100, 'Jan 2023'),
    ('s1002', 200, 'Feb 2023'),
    ('s1002', 300, 'Mar 2023'),
    ('s1003', 1000, 'Jan 2023'),
    ('s1003', 2000, 'Feb 2023'),
    ('s1003', 3000, 'Mar 2023'),
    ('s1004', 10000, 'Jan 2023'),
    ('s1004', 20000, 'Feb 2023'),
    ('s1004', 30000, 'Mar 2023');
SELECT * FROM SongsLogs;

-- --------------------------------------------------------------------------------

CREATE TABLE SongBelongsTo(
                              songID          VARCHAR(255) NOT NULL,
                              songGenreName   VARCHAR(255) NOT NULL,
                              PRIMARY KEY (songID, songGenreName),
                              FOREIGN KEY  (songID)
                                  REFERENCES Songs (songID)
                                  ON UPDATE CASCADE,
                              FOREIGN KEY (songGenreName)
                                  REFERENCES SongGenres (songGenreName)
                                  ON UPDATE CASCADE
);
INSERT INTO SongBelongsTo(songID, songGenreName)
VALUES
    ('s1001', 'Classical'),
    ('s1002', 'Rock'),
    ('s1003', 'Pop'),
    ('s1004', 'Classical');
SELECT * FROM SongBelongsTo;

-- --------------------------------------------------------------------------------

-- Assuming there is only one payment to earner per month.
CREATE TABLE Pays(
                     earnerID  VARCHAR(255) NOT NULL,
                     amount    DOUBLE(9, 2) NOT NULL,
    monthYear VARCHAR(255) NOT NULL,
    PRIMARY KEY (earnerID, monthYear),
    FOREIGN KEY (earnerID)
        REFERENCES Earners (earnerID)
        ON UPDATE CASCADE
);

INSERT INTO Pays(earnerID, amount, monthYear)
VALUES
    ('ph6001', 20, 'Jan 2023'),
    ('ph6001', 30, 'Feb 2023'),
    ('ph6001', 40, 'Mar 2023'),
    ('rl3001', 3.3, 'Jan 2023'),
    ('rl3001', 6.6, 'Feb 2023'),
    ('rl3001', 9.9, 'Mar 2023'),
    ('rl3002', 330, 'Jan 2023'),
    ('rl3002', 660, 'Feb 2023'),
    ('rl3002', 990, 'Mar 2023');

SELECT * FROM Pays;

-- --------------------------------------------------------------------------------

CREATE TABLE PaysArtists(
                            artistId  VARCHAR(255) NOT NULL,
                            amount    DOUBLE(9, 2) NOT NULL,
    monthYear VARCHAR(255) NOT NULL,
    PRIMARY KEY (artistId, monthYear),
    FOREIGN KEY (artistId)
        REFERENCES Artists (artistID)
        ON UPDATE CASCADE
);

INSERT INTO PaysArtists(artistId, amount, monthYear)
VALUES
    ('ar2001', 4.2, 'Jan 2023'),
    ('ar2001', 8.4, 'Feb 2023'),
    ('ar2001', 12.6, 'Mar 2023'),
    ('ar2002', 703.5, 'Jan 2023'),
    ('ar2002', 1547, 'Feb 2023'),
    ('ar2002', 2320.5, 'Mar 2023');

SELECT * FROM PaysArtists;

-- Pays PodcastHost when a specific podcast host is added.
# INSERT INTO Pays(serviceName, earnerID, month, year, amount)
#     (SELECT Earners.ID,
#             PodcastHosts.earnerId,
#             MONTH(NOW()),
#             YEAR(NOW()),
#             PodcastHosts.flatFee + (PodcastHosts.adBonus * podcastEpisodeAdvertisementCount) AS owed
#      FROM PodcastEpisodes
#               LEFT JOIN Runs ON Runs.podcastName = PodcastEpisodes.podcastName
#               LEFT JOIN PodcastHosts on Runs.earnerId = PodcastHosts.earnerId
#               LEFT JOIN Earners on Earners.earnerID = PodcastHosts.earnerId
#      WHERE PodcastEpisodes.podcastName = 'Oprah Super Soul'
#        AND podcastEpisodeTitle = 'S1 E2');
#
# SELECT * FROM Pays;

-- --------------------------------------------------------------------------------


/*CREATE TABLE Royalties
(
    earnerID        INT          NOT NULL,
    artistID        INT          NOT NULL,
    albumName       VARCHAR(255) NOT NULL,
    songTitle       VARCHAR(255) NOT NULL,
    songReleaseDate DATE         NOT NULL,
    amount          DOUBLE(9, 2) NOT NULL,
    monthYear       VARCHAR(255) NOT NULL,
    PRIMARY KEY (earnerId, artistId, albumName, songTitle, songReleaseDate, monthYear),
    FOREIGN KEY (earnerId)
        REFERENCES RecordLabels (earnerId)
        ON UPDATE CASCADE,
    FOREIGN KEY (artistId)
        REFERENCES Artists (artistId)
        ON UPDATE CASCADE,
    FOREIGN KEY (songID)
        REFERENCES Songs (songID)
        ON UPDATE CASCADE
);*/


-- ------------------------------------------------

CREATE TABLE Podcasts(
                         podcastId               varchar(255) NOT NULL,
                         podcastName             varchar(255) NOT NULL,
                         podcastEpisodeCount     int,
                         flatFeePerEpisode       DOUBLE(9, 2) NOT NULL,
    podcastRating           DECIMAL(2, 1),
    podcastTotalSubscribers int,
    podcastLanguage         varchar(255),
    podcastCountry          varchar(255),
    PRIMARY KEY (podcastId)
);

INSERT INTO Podcasts(podcastId, podcastName, podcastEpisodeCount, flatFeePerEpisode, podcastRating,
                     podcastTotalSubscribers, podcastLanguage, podcastCountry)
VALUES
    ('p5001','Mind Over Matter: Exploring the Power of the Human Mind', 5, 10, 4.5, 10, 'English', 'United States');
SELECT * FROM Podcasts;

-- ------------------------------------------------

CREATE TABLE PodcastEpisodes(
                                podcastEpisodeId                 varchar(255) NOT NULL,
                                podcastEpisodeTitle              varchar(255) NOT NULL,
                                podcastId                        varchar(255) NOT NULL,
                                podcastEpisodeListeningCount     int,
                                podcastEpisodeAdvertisementCount int,
                                podcastEpisodeDuration           TIME,
                                podcastEpisodeReleaseDate        DATE,
                                FOREIGN KEY (podcastId) REFERENCES Podcasts (podcastId),
                                PRIMARY KEY (podcastEpisodeId)
);
INSERT INTO PodcastEpisodes(podcastEpisodeId, podcastEpisodeTitle, podcastId, podcastEpisodeListeningCount,
                            podcastEpisodeAdvertisementCount, podcastEpisodeDuration, podcastEpisodeReleaseDate)
VALUES
    ('pe7001', 'The Science of Mindfulness', 'p5001', 100, null, '1:22:15', '2018-01-01'),
    ('pe7002', 'Unlocking Your Potential', 'p5001', 200, null, '1:20:30', '2018-02-01');
SELECT * FROM PodcastEpisodes;

-- --------------------------------------------------------------------------------

CREATE TABLE PodcastSponsors(
    podcastSponsorName varchar(255) PRIMARY KEY
);
INSERT INTO PodcastSponsors(podcastSponsorName)
VALUES
    ('ExpressVPN'),
    ('ZipRecruiter'),
    ('Audible'),
    ('DoorDash'),
    ('Apple'),
    ('IBM'),
    ('CapitalOne'),
    ('BetterHelp'),
    ('Comcast');
SELECT * FROM PodcastSponsors;

-- --------------------------------------------------------------------------------

CREATE TABLE PodcastSponsoredBy(
                                   podcastSponsorName varchar(255),
                                   podcastId          varchar(255),
                                   PRIMARY KEY (podcastSponsorName, podcastId),
                                   FOREIGN KEY (podcastSponsorName)
                                       REFERENCES PodcastSponsors (podcastSponsorName)
                                       ON UPDATE CASCADE,
                                   FOREIGN KEY (podcastId)
                                       REFERENCES Podcasts (podcastId)
                                       ON UPDATE CASCADE
);
INSERT INTO PodcastSponsoredBy(podcastSponsorName, podcastId)
VALUES
    ('DoorDash', 'p5001'),
    ('Audible', 'p5001'),
    ('ExpressVPN', 'p5001');
SELECT * FROM PodcastSponsoredBy;

-- --------------------------------------------------------------------------------

CREATE TABLE PodcastGenres(
    podcastGenreName varchar(255) PRIMARY KEY
);
INSERT INTO PodcastGenres(podcastGenreName)
VALUES
    ('Crime'),
    ('Comedy'),
    ('Business'),
    ('Kids'),
    ('Sports'),
    ('News'),
    ('Interview'),
    ('History'),
    ('Politics');
SELECT * FROM PodcastGenres;

-- --------------------------------------------------------------------------------

CREATE TABLE SpecialGuests(
                              guestName varchar(255) NOT NULL,
                              PRIMARY KEY (guestName)
);
INSERT INTO SpecialGuests(guestName)
VALUES
    ('James Bond'),
    ('Tony Stark'),
    ('Elon Musk'),
    ('Tim Cook'),
    ('Penelope Cruz'),
    ('Shakira');
SELECT * FROM SpecialGuests;

-- --------------------------------------------------------------------------------

CREATE TABLE Runs(
                     podcastHostEarnerId varchar(255) NOT NULL,
                     podcastId           varchar(255) NOT NULL,
                     PRIMARY KEY (podcastHostEarnerId, podcastId),
                     FOREIGN KEY (podcastHostEarnerId)
                         REFERENCES PodcastHosts (podcastHostEarnerId)
                         ON UPDATE CASCADE,
                     FOREIGN KEY (podcastId)
                         REFERENCES Podcasts (podcastId)
                         ON UPDATE CASCADE
);
INSERT INTO Runs(podcastHostEarnerId, podcastId)
VALUES
    ('ph6001', 'p5001');
SELECT * FROM Runs;

-- --------------------------------------------------------------------------------

CREATE TABLE PodcastBelongsTo(
                                 podcastGenreName varchar(255),
                                 podcastId      varchar(255),
                                 PRIMARY KEY (podcastGenreName, podcastId),
                                 FOREIGN KEY (podcastGenreName)
                                     REFERENCES PodcastGenres (podcastGenreName)
                                     ON UPDATE CASCADE,
                                 FOREIGN KEY (podcastId)
                                     REFERENCES Podcasts (podcastId)
                                     ON UPDATE CASCADE
);
INSERT INTO PodcastBelongsTo(podcastGenreName, podcastId)
VALUES
    ('Interview', 'p5001');
SELECT * FROM PodcastBelongsTo;

-- --------------------------------------------------------------------------------

CREATE TABLE Features(
                         guestName             varchar(255)          NOT NULL,
                         podcastEpisodeId         varchar(255) NOT NULL,
                         PRIMARY KEY (guestName, podcastEpisodeId),
                         FOREIGN KEY (guestName)
                             REFERENCES SpecialGuests (guestName)
                             ON UPDATE CASCADE,
                         FOREIGN KEY (podcastEpisodeId)
                             REFERENCES PodcastEpisodes (podcastEpisodeId)
                             ON UPDATE CASCADE
);
INSERT INTO Features(guestName, podcastEpisodeId)
VALUES
    ('James Bond', 'pe7001'),
    ('Penelope Cruz', 'pe7001'),
    ('Tony Stark', 'pe7002');
SELECT * FROM Features;

-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------
-- Payments SQL
-- All users pay the streaming service.
INSERT INTO StreamingServiceMonthlyRevenue (monthYear, revenue) VALUES((DATE_FORMAT(NOW(), '%b %Y')), (SELECT SUM(monthlySubscriptionFee) FROM Users WHERE subscriptionIsActiveStatus IS TRUE)) ON DUPLICATE KEY UPDATE
    revenue=revenue+(SELECT SUM(monthlySubscriptionFee) FROM Users WHERE subscriptionIsActiveStatus IS TRUE);
-- Pays all Podcast Hosts.
INSERT INTO Pays (earnerID, amount, monthYear)
SELECT * FROM (SELECT PodcastHosts.podcastHostEarnerId,
                      SUM(PodcastHosts.flatFee + (PodcastHosts.adBonus * IFNULL(podcastEpisodeAdvertisementCount, 0))) AS owed,
                      DATE_FORMAT(NOW(), '%b %Y')                                                                      AS monthYearEntry
               FROM PodcastEpisodes
                        LEFT JOIN Runs ON Runs.podcastId = PodcastEpisodes.podcastId
                        LEFT JOIN PodcastHosts on Runs.podcastHostEarnerId = PodcastHosts.podcastHostEarnerId
                        LEFT JOIN Earners on Earners.earnerID = PodcastHosts.podcastHostEarnerId
               GROUP BY Runs.podcastHostEarnerId, monthYearEntry) as dt
    ON DUPLICATE KEY UPDATE amount=amount+owed;
-- Updates revenue for the streaming service for current month by deducting what the podcast hosts have been paid for the month.
INSERT INTO StreamingServiceMonthlyRevenue (SELECT DATE_FORMAT(NOW(), '%b %Y'), (SELECT 0-SUM(amount) from Pays WHERE monthYear = DATE_FORMAT(NOW(), '%b %Y') AND earnerID IN (SELECT earnerID FROM PodcastHosts) GROUP BY monthYear)) ON DUPLICATE KEY UPDATE
    revenue = revenue - (SELECT SUM(amount) from Pays WHERE monthYear = DATE_FORMAT(NOW(), '%b %Y') AND earnerID IN (SELECT earnerID FROM PodcastHosts) GROUP BY monthYear);

-- Pays all record labels.
INSERT INTO Pays (earnerID, amount, monthYear)
SELECT * FROM (SELECT RecordLabels.earnerID AS eID,
                      SUM(IFNULL(Songs.songRoyaltyRatePerPlay, 0)*IFNULL(Songs.playCountCurrentMonth, 0)) AS owed,
                      DATE_FORMAT(NOW(), '%b %Y') AS currentMonthYear
               FROM RecordLabels
                        LEFT JOIN Earners ON Earners.earnerID = RecordLabels.earnerID
                        LEFT JOIN Artists on RecordLabels.earnerID = Artists.recordLabelEarnerID
                        LEFT JOIN Sings on Artists.artistID = Sings.artistID
                        LEFT JOIN Songs ON Sings.songID = Songs.songID
               GROUP BY eID) as dt
    ON DUPLICATE KEY UPDATE amount=amount+owed;
-- Updates revenue for the streaming service for current month by deducting what the record labels have been paid for the month.
INSERT INTO StreamingServiceMonthlyRevenue (SELECT DATE_FORMAT(NOW(), '%b %Y'), (SELECT 0-SUM(amount) from Pays WHERE monthYear = DATE_FORMAT(NOW(), '%b %Y') AND earnerID IN (SELECT earnerID FROM RecordLabels) GROUP BY monthYear)) ON DUPLICATE KEY UPDATE
    revenue = revenue - (SELECT SUM(amount) from Pays WHERE monthYear = DATE_FORMAT(NOW(), '%b %Y') AND earnerID IN (SELECT earnerID FROM RecordLabels) GROUP BY monthYear);



SELECT * FROM Pays;



-- --------------------------------------------------------------------------------
-- --------------------------------------------------------------------------------


-- Insert Song info.
INSERT INTO Songs VALUES ('Complete Piano Sonatas', 'Piano Sonata 1', '1770-12-12', 0, 'English', 0.30, 0, '0:3:30');
UPDATE Songs SET songPlayCount=45 where albumName='Fearless' AND songTitle='White Horse';
DELETE from Songs WHERE albumName='Origins' AND songTitle='West Coast';

INSERT INTO Artists VALUES (1, 'Taylor Swift', 'Pop', 'Singer', TRUE, 5000000, 'US');
UPDATE Artists SET artistMonthlyListeners=5000300 where artistID=1;
DELETE from Artists WHERE artistID=4;

INSERT INTO Songs(albumName, songTitle, songReleaseDate, songPlayCount, songLanguage, songRoyaltyRate, isSongRoyaltyPaid, songDuration)
VALUES
    ('Origins', 'West Coast', '1770-12-12', 0, 'English', 0.32, 0, '0:3:37');
INSERT INTO Sings(artistID, albumName, songTitle, songReleaseDate)
VALUES
    (2, 'Origins', 'West Coast', '1770-12-12');


INSERT INTO PodcastHosts VALUES (1, 'jorog@gmail.com', 'Rogan', 'Joe', '9195154000', 'San Diego', 5000, 200);
UPDATE PodcastHosts SET podcastHostCity='Dallas' WHERE earnerId=5;
DELETE from PodcastHosts where earnerId = 1;

INSERT INTO PodcastEpisodes(podcastName, podcastEpisodeTitle, podcastEpisodeDuration, podcastEpisodeReleaseDate, podcastEpisodeListeningCount, podcastEpisodeAdvertisementCount)
VALUES
    ('Joe Rogan Podcast', 'Episode 1', "1:22:15", '2018-01-01', 3400, 20),
    ('Joe Rogan Podcast', 'Episode 2', "1:20:30", '2018-02-01', 3545, 24),
    ('Joe Rogan Podcast', 'Episode 3', "1:31:05", '2018-03-01', 3480, 18);
UPDATE PodcastEpisodes SET podcastEpisodeListeningCount=podcastEpisodeListeningCount+100 where podcastName='Hablas Con Maron' AND podcastEpisodeTitle='Episodio Dos';
DELETE from PodcastEpisodes WHERE podcastName='Hablas con Maron' AND podcastEpisodeTitle='Episodio Tres';


INSERT INTO Artists (earnerID, artistName, artistPrimaryGenre, artistType,artistStatus, artistMonthlyListeners, artistCountry)
VALUES
    (2, 'Taylor Swift', 'Pop', 'Singer', TRUE, 5000000, 'US');
INSERT INTO Runs VALUES (1, 'Joe Rogan Podcast');
INSERT INTO PodcastEpisodes
VALUES
    ('Hablas con Maron', 'Episodio Dos', "0:58:45", '2022-06-28', 293, 13);



-- ------------------------------------------------------------------------------------------------------


SHOW TABLES;

DROP TABLE IF EXISTS Features;
DROP TABLE IF EXISTS PodcastBelongsTo;
DROP TABLE IF EXISTS Runs;
DROP TABLE IF EXISTS SpecialGuests;
DROP TABLE IF EXISTS PodcastGenres;
DROP TABLE IF EXISTS PodcastSponsoredBy;
DROP TABLE IF EXISTS PodcastSponsors;
DROP TABLE IF EXISTS PodcastEpisodes;
DROP TABLE IF EXISTS Podcasts;
DROP TABLE IF EXISTS PaysArtists;
DROP TABLE IF EXISTS Pays;
DROP TABLE IF EXISTS SongBelongsTo;
DROP TABLE IF EXISTS SongsLogs;
DROP TABLE IF EXISTS Collaborates;
DROP TABLE IF EXISTS Sings;
DROP TABLE IF EXISTS Songs;
DROP TABLE IF EXISTS Albums;
DROP TABLE IF EXISTS Artists;
DROP TABLE IF EXISTS SongGenres;
DROP TABLE IF EXISTS PodcastHosts;
DROP TABLE IF EXISTS RecordLabels;
DROP TABLE IF EXISTS Earners;
DROP TABLE IF EXISTS StreamingServiceMonthlyRevenue;
DROP TABLE IF EXISTS Users;

SHOW TABLES;

-- Find podcast episodes given the podcast
SELECT Podcasts.podcastName, podcastEpisodeTitle FROM Podcasts LEFT JOIN PodcastEpisodes ON PodcastEpisodes.podcastName = Podcasts.podcastName
WHERE Podcasts.podcastName = 'Joe Rogan Podcast';
SELECT Podcasts.podcastName, PodcastEpisodes.podcastEpisodeTitle FROM PodcastEpisodes LEFT JOIN Podcasts ON PodcastEpisodes.podcastName = Podcasts.podcastName
WHERE Podcasts.podcastName = 'Joe Rogan Podcast';

-- Find songs given the artist
SELECT Songs.songTitle, Songs.albumName, Songs.songReleaseDate FROM Songs
                                                                        LEFT JOIN Sings ON Sings.albumName = Songs.albumName
    AND Sings.songTitle = Songs.songTitle
    AND Songs.songReleaseDate = Sings.songReleaseDate
                                                                        LEFT JOIN Artists on Sings.artistID = Artists.artistID
                                                                        LEFT JOIN Albums on Songs.albumName = Albums.albumName
WHERE Artists.artistName = 'Imagine Dragons';

SELECT Songs.songTitle, Songs.albumName, Songs.songReleaseDate
FROM Songs
         LEFT JOIN Sings ON Sings.albumName = Songs.albumName
    AND Sings.songTitle = Songs.songTitle
    AND Songs.songReleaseDate = Sings.songReleaseDate
         LEFT JOIN Artists on Sings.artistID = Artists.artistID
         LEFT JOIN Albums on Songs.albumName = Albums.albumName
WHERE Albums.albumName = 'Folklore';

SELECT Songs.songTitle, Songs.albumName, Songs.songReleaseDate, artistName
FROM Songs
         LEFT JOIN Sings ON Sings.albumName = Songs.albumName
    AND Sings.songTitle = Songs.songTitle
    AND Songs.songReleaseDate = Sings.songReleaseDate
         LEFT JOIN Artists on Sings.artistID = Artists.artistID
         LEFT JOIN Albums on Songs.albumName = Albums.albumName
WHERE Albums.albumName = 'Folklore' OR Artists.artistName = 'Taylor Swift';

SELECT * FROM StreamingService;
SELECT * FROM PaysSubscriptionAmounts;
DESCRIBE PaysSubscriptionAmounts;

SELECT streamingService, SUM(monthlySubscriptionFee) FROM Users WHERE subscriptionStatus = 1 GROUP BY streamingService;

SELECT * FROM StreamingService
                  LEFT JOIN (SELECT streamingService, SUM(monthlySubscriptionFee) as owed FROM Users WHERE subscriptionStatus = 1 GROUP BY streamingService)
    AS main ON StreamingService.serviceName = main.streamingService;
-- Users paying streaming service.
-- -- Part 1
UPDATE StreamingService
    LEFT JOIN (SELECT streamingService, SUM(monthlySubscriptionFee) AS owed
    FROM Users
    WHERE subscriptionStatus = 1
    GROUP BY streamingService)
    AS main ON StreamingService.serviceName = main.streamingService
    SET StreamingService.serviceBalance = StreamingService.serviceBalance + main.owed;
-- -- Part 2
INSERT INTO PaysSubscriptionAmounts
    (SELECT streamingService, userEmail, MONTH(NOW()), YEAR(NOW()), monthlySubscriptionFee
     FROM Users
     WHERE subscriptionStatus = 1);


-- Streaming service makes all payments to record labels.
INSERT INTO Pays(serviceName, earnerID, month, year, amount)
    (SELECT Earners.streamingService                                               AS ss,
            RecordLabels.earnerID                                                  AS eID,
         MONTH(NOW()),
         YEAR(NOW()),
         SUM(IFNULL(Songs.songRoyaltyRate, 0) * IFNULL(Songs.songPlayCount, 0)) AS owed
     FROM RecordLabels
         LEFT JOIN Earners ON Earners.earnerID = RecordLabels.earnerID
         LEFT JOIN Artists on RecordLabels.earnerID = Artists.earnerID
         LEFT JOIN Sings on Artists.artistID = Sings.artistID
         LEFT JOIN Songs ON Sings.albumName = Songs.albumName and
         Sings.songTitle = Songs.songTitle and
         Sings.songReleaseDate =
         Songs.songReleaseDate
     GROUP BY ss, eID);



DESCRIBE PaysSubscriptionAmounts;


SELECT * FROM StreamingService
                  LEFT JOIN PaysSubscriptionAmounts ON StreamingService.serviceName = PaysSubscriptionAmounts.serviceName;

DESCRIBE Artists;
DESCRIBE Albums;
DESCRIBE Sings;
DESCRIBE Songs;

SELECT * FROM Sings;
SELECT * FROM Songs;
SELECT * FROM Albums;

DESCRIBE Podcasts;
DESCRIBE PodcastEpisodes;

DROP TABLE Users;
DROP TABLE PaysSubscriptionAmounts;
DESCRIBE StreamingService;