!/bin/bash
FILES=./downloads/NYSE/NYSE_daily_prices_*.csv
for f in $FILES
do
    echo "Processing $f file..."
    # ls -l $f
    mongoimport --db nysedb --collection stocks --type csv --headerline --file $f
done
