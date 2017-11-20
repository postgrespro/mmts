./dtmbench -i -a 100000 -c "dbname=regression host=localhost port=5434 sslmode=disable"

./dtmbench  \
-c "dbname=regression host=localhost port=5432 sslmode=disable" \
-c "dbname=regression host=localhost port=5433 sslmode=disable" \
-c "dbname=regression host=localhost port=5434 sslmode=disable" \
-n 10000 -a 100000 -w 30 -r 10 $*
