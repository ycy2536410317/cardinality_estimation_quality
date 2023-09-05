python3 cardinality_estimation_quality.py 'host=localhost port=5432 user=imdb dbname=imdb' ../postgresql-12.5/job/

python3 slow_down.py 'host=localhost port=5432 user=imdb dbname=imdb' ../postgresql-12.5/job ../postgresql-12.5/job-hint

python3 plan_space.py 'host=localhost port=5432 user=imdb dbname=imdb' ../PGDev/postgresql-12.5/job-hint 