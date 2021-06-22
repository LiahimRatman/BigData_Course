CREATE TABLE IF NOT EXISTS movies_by_genre_rating (
    movieid text,
    year int,
    genre text,
    title text,
    rating float,
    PRIMARY KEY (genre, year, rating, movieid)
    )
    WITH CLUSTERING ORDER BY (year DESC, rating desc);