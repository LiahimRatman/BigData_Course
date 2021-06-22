CREATE TABLE IF NOT EXISTS movies_by_genre (
    movieid text,
    year int,
    genre text,
    title text,
    PRIMARY KEY (genre, year, movieid)
    )
    WITH CLUSTERING ORDER BY (year DESC);
