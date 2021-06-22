SELECT MIN(rating), AVG(rating), MAX(rating) FROM movies_by_genre_rating
where genre = 'Sci-Fi' and year >= 2000;
