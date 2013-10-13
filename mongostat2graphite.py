#!/usr/bin/python
# this script is by most was writeen by joe.drumgoole@10gen.com (https://github.com/jdrumgoole/mstat_to_csv) and was forken by Oded Maimon to add graphite output support (https://github.com/jdrumgoole/mstat_to_csv)
#
# Simple program to parse mongostat output into one of the following formats:
#   - a sensible CSV format for ingestion by Google Docs or Excel
#   - graphite format for sending the data to carbon
# 
# Mongostat does some things that make it difficult to ingest into Excel or a Google Spreadsheet,
# including:
#
#  Field labels that have spaces and special characters in them
#  Varying column widths
#  Columns within columns
#  No defined column seperator
#  Automatic insert of header fields ever 10 lines or so.
#
# mongostat2graphite.py can read from stdin (via a pipe) or a file and write to stdout or a file.
#
# By default it converts all input into CSV format or most input to graphite (removes the time and set columns when using graphite format)
#
# You can direct its output to a file with the --output <filename> argument.
#
# You can read from a file with with the --input <filenamne> argument.
#
# You can select a specific column to output by using the the --columns <column name> argument.
# The --columns argument can be repeated to select more than one column. The columns are output in the
# order of the arguments.
#
# You can list the canonical column names with --listcolumns
#
# You can add a rowcount column using the --rowcount argument. This allows the output to be normalised
# on a standard interval so different runs of the program can be graphed on the same timescale. (rowcount is disabled by default for graphite output)
#
# joe.drumgoole@10gen.com 
#

import sys
import re
import argparse
import socket
import time

# if ordereddict is available use it (i.e when using py 2.4 or 2.6)
# use pip-2.6 install ordereddict to install this module
try:
    import ordereddict as collections
except ImportError:
    import collections

column_order = [   'insert', # 0
                   'query',        # 1
                   'update',       # 2
                   'delete',       # 3
                   'getmore',      # 4
                   'command',      # 5
                   'flushes',      # 6
                   'mapped',       # 7
                   'vsize',        # 8
                   'res',          # 9
                   'faults',       # 10
                   'locked db',    # 11
                   'idx miss %',   # 12
                   'qr|qw',        # 13
                   'ar|aw',        # 14
                   'netIn',        # 15
                   'netOut',       # 16
                   'conn',         # 17
                   'set',          # 18
                   'repl',         # 19
                   'time' ]        # 20

column_indexes = { 'insert'    : 0,
                   'query'      : 1,
                   'update'     : 2,
                   'delete'     : 3,
                   'getmore'    : 4,
                   'command'    : 5,
                   'flushes'    : 6,
                   'mapped'     : 7,
                   'vsize'      : 8,
                   'res'        : 9,
                   'faults'     : 10,
                   'locked db'  : 11,
                   'idx miss %' : 12,
                   'qr|qw'      : 13,
                   'ar|aw'      : 14,
                   'netIn'      : 15,
                   'netOut'     : 16,
                   'conn'       : 17,
                   'set'        : 28,
                   'repl'       : 10,
                   'time'       : 20 }  #also set and repl but not for this version


def parseHeader( canonical_columns, x ) :

    headers = collections.OrderedDict() 
    
    position = 0
    for i in canonical_columns:
        if re.search( i, x ) :
#            print "found %s in %s" % ( i, x )
            headers[ i ] = position 
            position += 1
 
    return headers

def parseLine( x ) :
    x =re.sub( '^[ ]+', "", x )
    x =re.sub( '[ ]+$', "", x ) 
    x= re.sub( '[ ]+', ',', x )
    x= re.sub( '\*', '', x )
    return x.split( ',' ) 

def processLine( x, actual_columns, selectors , format = "csv" , graphiteprefix = "mongodb.stats") :
    return  processColumns( parseLine( x ), actual_columns, selectors , format, graphiteprefix)

def processHeader( actual_columns, selectors ) :
    
    output_columns = []
    for i in selectors:
        if i in actual_columns :
            output_columns.append( i ) 

    return ",".join( output_columns ) 
        


def processColumns( column_data, actual_columns, selectors, format = "csv", graphiteprefix = "mongodb.stats") :
#
# Take a row of column data and a list of selected columns and filter out the columns
# that will be output based on the selectors. Data has been pre-cleaned so selectors is non-zero
# and valid.
#
    
    output_columns = []
    currTime = str(time.time())
    #print ( "selectors : %s" % selectors )  
    for i in selectors :
        #print "index : %s %d " % ( i, actual_columns[ i ] )
        if i in actual_columns :
            if format == "csv":
                    output_columns.append( column_data[ actual_columns[ i ]]) 
            else:
                data = column_data[ actual_columns[ i ]]
                # sepcial handler for repl column
                if i == "repl":
                    # handle it in the followin way:
                    #    PRI - primary (master) = 1
                    #    SEC - secondary = 2
                    #    REC - recovering = 3
                    #    UNK - unknown = 4
                    #    SLV - slave = 5
                    #    RTR - mongos process ("router") = 6
                    # any other value will be 7
                    if data == "PRI":
                        data = 1
                    elif data == "SEC":
                        data = 2
                    elif data == "REC":
                        data = 3
                    elif data == "UNK":
                        data = 4
                    elif data == "SLV":
                        data = 5
                    elif data == "RTR":
                        data = 6
                    else:
                        data = 7
                if i == "netOut" or i == "netIn":
                    st = data[-1]
                    if st == "b":
                        data = float(data[:-1])
                        pass # dont do anything
                    elif st == "k":
                        data = float(data[:-1])
                        data = data * 1024
                    elif st == "m":
                        data = float(data[:-1])
                        data = data * 1024 * 1024
                    elif st == "g":
                        data = float(data[:-1])
                        data = data * 1024 * 1024 * 1024
                    elif st == "t":
                        data = float(data[:-1])
                        data = data * 1024 * 1024 * 1024 * 1024
                if i == "mapped" or i == "vsize" or i == "res":
                    st = data[-1]
                    if st == "m":
                        data = float(data[:-1])
                        pass
                    elif st == "g":
                        data = float(data[:-1])
                        data = data * 1024 
                    elif st == "t": 
                        data = float(data[:-1])
                        data = data * 1024 * 1024
                    elif st == "p":
                        data = float(data[:-1])
                        data = data * 1024 * 1024 * 1024
                        
                        
                sep = ""
                # if data have * in first field, change the data to include the word replicated
                if not isinstance( data, ( int, long, float ) ) and data[0] == "*":
                    data = data.strip("*")
                    sep = ".replicated"

                if i == "locked db":
                    data = data.split(":")
                    if data[0] == ".":
                        data[0] = "global"
                    sep = "." + data[0]
                    data = data[1].strip("%")
                    
                if (i == "command" or i == "qr|qw" or i == "ar|aw") and (not isinstance( data, ( int, long, float ) ) and data.find("|") != -1) :
                    data = data.split("|")
                    if i == "command":
                        i1 = "command.local"
                        i2 = "command.replicated"
                    elif i == "qr|qw":
                        i1 = "qr"
                        i2 = "qw"
                    elif i == "ar|aw":
                        i1 = "ar"
                        i2 = "aw"
                    
                    output_columns.append( graphiteprefix + "." + i1 + sep + " " + str(data[0]) + " " + currTime)
                    output_columns.append( graphiteprefix + "." + i2 + sep + " " + str(data[1]) + " " + currTime)
                else:
                    output_columns.append( graphiteprefix + "." + i.replace(" ","_") + sep + " " + str(data) + " " + currTime)

    if format == "csv":
        return ','.join( output_columns ) + "\n"
    else:
        return '\n'.join( output_columns ) + "\n"
    
if __name__ == '__main__':

    parser = argparse.ArgumentParser( description="Program to parse the output of mongostat into a CSV file" )

    parser.add_argument('--version', action='version', version='%(prog)s 0.2.2 beta')

    parser.add_argument( "--output", 
                         help="Define an output file to write to (default is stdout)",
                         default="stdout" )

    parser.add_argument( "--append", 
                         action="store_true",
                         help="Append output to the file specified by --output",
                         default=False )

    parser.add_argument( "--noheaders", 
                         action="store_true",
                         help="Don't output header columns (useful with --append)",
                         default=False )

    parser.add_argument( "--input",
                         help="Define an input file tor read from (default is stdin)",
                         default="stdin"  )

    parser.add_argument( "--columns",
                         action='append',
                         help="Only output named columns in the order they appear on the command line", 
                         default=[] )

    parser.add_argument( "--rowcount",
                         action='store_true',
                         help="add a column to the left that numbers each row of output", 
                         default=False )

    parser.add_argument( "--listallcolumns",
                         action="store_true",
                         help="list out canonical column headings and exit", 
                         default=False )
    
    parser.add_argument( "--listcolumns",
                         action="store_true",
                         help="list out columns in current output and exit", 
                         default=False )

    parser.add_argument( "--format",
                         help="csv/graphite (default csv)", 
                         choices=['csv', 'graphite'],
                         default="csv" )

    parser.add_argument( "--graphiteprefix",
                         help="graphite metrics prefix ( default: mongodb.stats ), it alwasys adds the hostname after prefix and before metric, i.e mongodb.stats.myhostname1.insert", 
                         default="mongodb.mongodb.stats" )

    args = parser.parse_args()

    if args.input == "stdin" :
        input_stream = sys.stdin 
    else :
        input_stream = open( args.input, "r" ) ;

    writeStr = "wb"
    if args.output == "stdout" :
        output_stream = sys.stdout 
    else :
        if args.append :
            writeStr = "ab" ;
            
        output_stream = open( args.output, writeStr )

    if args.listallcolumns :
        output_stream.write( ','.join( column_order )) 
        output_stream.write( "\n" ) 
        output_stream.close()
        sys.exit( 0 )

    x = input_stream.readline()  #
 
    if x.startswith( "connected" ) :
        x = input_stream.readline()  # strip of connected to

    if x.startswith( "insert" ) :
        actual_columns = parseHeader( column_order, x.rstrip( "\n" ))
        selected_columns = []

        #
        # if the user selects a column which is not in the output we ignore it.
        #
        

        if len( args.columns ) == 0 :
            # just use the list of columns we parsed if the user didn't select any
            selected_columns = actual_columns 
        else:
            for i in args.columns :
                if i in actual_columns  :
                    selected_columns.append( i ) 
                else :
                    sys.stderr.write( "Warning : you selected display of a column, '%s', which is not in the mongostat output\n" % i )

        # we always use noheaders and no row count in graphite
        # we also remove few columns by default (string columns)
        if args.format == "graphite": 
            args.noheaders = True
            args.rowcount = False
            del selected_columns["set"]
            del selected_columns["time"]
            #selected_columns.remove("set")
            #selected_columns.remove("time")
            
        if args.noheaders :
            pass
        else:
            if args.rowcount :
                output_stream.write( "count," ) 

            output_stream.write( processHeader( actual_columns, selected_columns )) # print headers once
            
            output_stream.write( "\n" ) 

            if args.listcolumns :
                sys.exit( 0 ) # we are done.

        x = input_stream.readline()  # 

    rowcounter = 1
    
    # add the hostname to graphite prefix
    graphiteprefix = args.graphiteprefix + "." + socket.gethostname()
    while x :
        if x.startswith( "insert" ) :
            x = input_stream.readline()
            continue # strip out occasional header lines

        if args.rowcount :
            output_stream.write( "%i," % rowcounter ) ;
            rowcounter += 1 

        output_stream.write( processLine( x.rstrip( "\n" ), actual_columns, selected_columns, args.format, graphiteprefix)) # print headers once
        
        output_stream.flush() 
#        print( x.rstrip( "\n" ))
        x = input_stream.readline()

    input_stream.close()
    output_stream.close()
    