SELECT `date`, count(*) as cnt from logs
group by `date`
order by cnt desc limit 10;
