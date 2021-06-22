CREATE TABLE IF NOT EXISTS movies_with_genre_index (
    movieid text,
    title text,
    year int,
    genres set<text>,
    PRIMARY KEY ((title, year), movieid)
);
CREATE INDEX genre_idx ON movies_with_genre_index ( genres );
