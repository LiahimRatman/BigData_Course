CREATE TABLE IF NOT EXISTS movies (
    movieid text,
    title text,
    year int,
    genres set<text>,
    PRIMARY KEY ((title, year), movieid)
);
