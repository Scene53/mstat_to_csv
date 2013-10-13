Mongostat to CSV Conversion Script
==================================

    usage: mongostat2graphite.py [-h] [--version] [--output OUTPUT] [--append]
                             [--noheaders] [--input INPUT] [--columns COLUMNS]
                             [--rowcount] [--listallcolumns] [--listcolumns]
                             [--format {csv,graphite}]
                             [--graphiteprefix GRAPHITEPREFIX]

Program to parse the output of mongostat into a CSV file

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --output OUTPUT       Define an output file to write to (default is stdout)
  --append              Append output to the file specified by --output
  --noheaders           Don't output header columns (useful with --append)
  --input INPUT         Define an input file tor read from (default is stdin)
  --columns COLUMNS     Only output named columns in the order they appear on
                        the command line
  --rowcount            add a column to the left that numbers each row of
                        output
  --listallcolumns      list out canonical column headings and exit
  --listcolumns         list out columns in current output and exit
  --format {csv,graphite}
                        csv/graphite (default csv)
  --graphiteprefix GRAPHITEPREFIX
                        graphite metrics prefix ( default: mongodb.stats ), it
                        alwasys adds the hostname after prefix and before
                        metric, i.e mongodb.stats.myhostname1.insert

Example usage:

Basic data sending to graphite instace:
    $ mongostat --host localhost --port 10001 5 | python26 ./mongostat2graphite.py --format graphite | nc graphite.host

	mongodb.stats.mymongohost1.insert 0 1381667304.65
    mongodb.stats.mymongohost1.query 0 1381667304.65
    mongodb.stats.mymongohost1.update 18 1381667304.65
    mongodb.stats.mymongohost1.delete 0 1381667304.65
    mongodb.stats.mymongohost1.getmore 0 1381667304.65
    mongodb.stats.mymongohost1.command.local 8 1381667304.65
    mongodb.stats.mymongohost1.command.replicated 0 1381667304.65
    mongodb.stats.mymongohost1.flushes 0 1381667304.65
    mongodb.stats.mymongohost1.mapped 51507.2 1381667304.65
    mongodb.stats.mymongohost1.vsize 103424.0 1381667304.65
    mongodb.stats.mymongohost1.res 4177.92 1381667304.65
    mongodb.stats.mymongohost1.faults 0 1381667304.65
    mongodb.stats.mymongohost1.locked_db.casino53 2.0 1381667304.65
    mongodb.stats.mymongohost1.idx_miss_% 0 1381667304.65
    mongodb.stats.mymongohost1.qr 0 1381667304.65
    mongodb.stats.mymongohost1.qw 0 1381667304.65
    mongodb.stats.mymongohost1.ar 0 1381667304.65
    mongodb.stats.mymongohost1.aw 0 1381667304.65
    mongodb.stats.mymongohost1.netIn 601.0 1381667304.65
    mongodb.stats.mymongohost1.netOut 14336.0 1381667304.65
    mongodb.stats.mymongohost1.conn 83 1381667304.65
    mongodb.stats.mymongohost1.repl 2 1381667304.65
	
The default case:

    JD10Gen:mstat_to_csv jdrumgoole$ mongostat | python mstat_to_csv.py 
    insert,query,update,delete,getmore,command,flushes,mapped,vsize,res,faults,locked db,idx miss %,qr|qw,ar|aw,netIn,netOut,conn,time
    0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:24:12
    0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:24:13
    0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:24:14
    0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:24:15
    0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:24:16

Just get the query column:

    JD10Gen:mstat_to_csv jdrumgoole$ mongostat | python mstat_to_csv.py --columns query
    query
    0
    0
    0
    0

Get the queries and vsize columns:

    JD10Gen:mstat_to_csv jdrumgoole$ mongostat | python mstat_to_csv.py --columns query --columns vsize
    query,vsize
    0,2.66g
    0,2.66g
    0,2.66g
    1,2.66g
    0,2.66g

Get the output and add a rowcount:

    JD10Gen:mstat_to_csv jdrumgoole$ mongostat | python mstat_to_csv.py --rowcount
    count,insert,query,update,delete,getmore,command,flushes,mapped,vsize,res,faults,locked db,idx miss %,qr|qw,ar|aw,netIn,netOut,conn,time
    1,0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:27:21
    2,0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:27:22
    3,0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:27:23
    4,0,0,0,0,0,1|0,0,80m,2.66g,143m,0,.:0.0%,0,0|0,0|0,62b,2k,1,14:27:24
    5,0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:27:25
    6,0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:27:26
    7,0,0,0,0,0,1|0,0,80m,2.66g,143m,0,local:0.0%,0,0|0,0|0,62b,2k,1,14:27:27

Get the output and write to a file:

    JD10Gen:mstat_to_csv jdrumgoole$ mongostat | python mstat_to_csv.py --rowcount --output mstat.out
