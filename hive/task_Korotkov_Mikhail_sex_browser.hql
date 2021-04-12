set hive.auto.convert.join=false;
set mapreduce.job.reduces=8;

SELECT browser, sum(if(sex='male', 1, 0)) as male, sum(if(sex='female', 1, 0)) as female FROM
logs join users
on (logs.ip = users.ip)
group by browser
limit 10;

-- hive -e "use made21q1_korotkov; SELECT browser, sum(if(sex='male', 1, 0)) as cnt_male, sum(if(sex='female', 1, 0)) as cnt_female FROM logs join users on (logs.ip = users.ip) group by browser limit 10;" > sex_browser.out
-- SELECT browser, sum(if(sex='male', 1, 0)) as male, sum(if(sex='female', 1, 0)) as female FROM logs join users on (logs.ip = users.ip) group by browser limit 10;
