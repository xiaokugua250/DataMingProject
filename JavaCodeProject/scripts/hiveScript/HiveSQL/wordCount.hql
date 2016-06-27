SELECT word ,count(1) as count FROM
(SELECT explode(split(line,"\s")) AS word FROM DOCS) w
GROUP BY word
ORDER BY word;
