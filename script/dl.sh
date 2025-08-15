curl -o sor-global-2024-01-01-full.parquet.zip \
    https://d3vax7phxnku8l.cloudfront.net/raw/pqt/data/tdb_data/global___full/daily_dumps_chunked/sor-global-2024-01-01-full.parquet.zip


# for day in `seq 1 30`; do
#    tmp_fname=sor-global-2023-09-`printf "%02d" $day`-full.parquet.zip;
#    wget -O data/$tmp_fname https://d3vax7phxnku8l.cloudfront.net/raw/pqt/data/tdb_data/global___full/daily_dumps_chunked/$tmp_fname;
#    # Optional: check sha1
#    # wget -O data/"$tmp_fname".sha1 https://d3vax7phxnku8l.cloudfront.net/raw/pqt/data/tdb_data/global___full/daily_dumps_chunked/"$tmp_fname".sha1;
#    # cd data
#    # sha1sum -c "$tmp_fname".sha1;
#    # cd ..
#    unzip -d /data/tdb_data/ data/"tmp_fname"
# done
