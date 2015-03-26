# MapReduce
Operating System Project2

Put all your server address into the config file

python worker.py 127.0.0.1:9001
python worker.py 127.0.0.1:9002
python worker.py 127.0.0.1:9003
python worker.py 127.0.0.1:9004
python worker.py 127.0.0.1:9005
python worker.py 127.0.0.1:9006
python worker.py 127.0.0.1:9007
python worker.py 127.0.0.1:9008

python master.py 127.0.0.1:9000


python mr_job.py 127.0.0.1:9000 wordcount.py 10240 2 england.txt wordcount_result text

#to see the improvements using an 26.3M file
python mr_job.py 127.0.0.1:9000 wordcount.py 102400 2 england.txt wordcount_result text

python mr_collect.py wordcount_result result

python mr_job.py 127.0.0.1:9000 encode.py 30000 2 england.small.txt encode_result binary

python mr_collect.py encode_result england.small.encoded.txt merge

python mr_job.py 127.0.0.1:9000 decode.py 30000 2 england.small.encoded.txt decode_result binary

python mr_collect.py decode_result england.small.decoded.txt merge


python mr_job.py 127.0.0.1:9000 error.py 30000 2 england.small.encoded.txt err_result binary

python mr_collect.py err_result england.small.encoded.err.txt merge

python mr_job.py 127.0.0.1:9000 check.py 30000 2 england.small.encoded.txt check_result binary
python mr_job.py 127.0.0.1:9000 check.py 30000 2 england.small.encoded.err.txt check_result binary

python mr_collect.py check_result england.small.encoded.chk.txt collect

python mr_job.py 127.0.0.1:9000 fix.py 30000 2 england.small.encoded.err.txt fix_result binary

python mr_collect.py fix_result england.small.encoded.fixed.txt merge

diff england.small.encoded.txt england.small.encoded.fixed.txt

