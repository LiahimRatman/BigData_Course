SELECT COUNT(*) FROM movies_with_genre_index
where genres CONTAINS 'Horror' and year >= 1980 and year <= 1990
ALLOW FILTERING;