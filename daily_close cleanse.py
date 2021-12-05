
import csv
import datetime
#import os#for 'lineterminator=os.linesep'

csv_columns = ["symbol","date","close","volume","open","high","low"]
#sample good and bad data>>>
#GOOGL,"2018/06/22","1169.2900","1710355.0000","1171.4950","1175.0000","1159.6500"
#AMZN,"14:27","1,716.91","3,019,620","1,715.40","1,720.87","1,708.52"

isSample = False;   #limits to read to 10 rows

csv.register_dialect('neo4j-csv_dialect', delimiter=',', escapechar='\\', doublequote=True) #, newline='') #lineterminator=os.linesep)


with open('daily_close.csv.dirty', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    line_count = 0
    with open (r'daily_close-clean.CSV', 'wt', newline='') as outputFile:
        writer_good = csv.DictWriter(outputFile, fieldnames=csv_columns, dialect='neo4j-csv_dialect')
        with open (r'error.CSV', 'wt', newline='') as errorFile:
            writer_bad = csv.DictWriter(errorFile, fieldnames=csv_columns, dialect='neo4j-csv_dialect')
    
            for row in csv_reader:
                isGoodRow = True;
                
                #(1st row only) - Print Header info
                if line_count == 0:
                    print(f'Column names are {", ".join(row)}')
                    writer_good.writeheader()

                #now, for each row...
                #print(f'{row["symbol"]},{row["date"]} --- {row["close"]} --- {row["volume"]}')

                #CLEANSING RULES BEGIN>>>
                
                # 1) Attempt parse date string   {row["date"]}
                #       Year with century as a decimal number:            %Y
                #       Month as a zero-padded decimal number:            %m
                #       Day of the month as a zero-padded decimal number: %d

                        # example:
                #          date_string = "Sat Feb 8 2020"
                #           datetime_object = datetime.strptime(date_string, "%a %b %d %Y")
                try:
                    datetime_object = datetime.datetime.strptime( row["date"] , "%Y/%m/%d")
                    #[debug] print("in ISO 8601 format: ", datetime_object.date().isoformat())
                    row["date"] = datetime_object.date().isoformat();
                except ValueError:
                    print("line: ", csv_reader.line_num, " >>> ERROR: Could not convert field into an date!" )
                    isGoodRow = False;

                # 2) test volume string for last four digits   {row["volume"]}
                if isGoodRow:
                    iDecPointIndex = row["volume"].find('.')
                    if ( iDecPointIndex > -1 ):
                        sDecComponent = row["volume"][(iDecPointIndex+1):]
                        iDecComponent = int( sDecComponent );
                        #print( "line: ", csv_reader.line_num, " >>> volume: Decimal Point found at pos: ", iDecPointIndex, "; Decimal Component (s): ", sDecComponent, "; Decimal Component (i): ", iDecComponent )
                        if ( iDecComponent > 0 ):
                            print( "ERROR: Volumne is not an integer!");
                            isGoodRow = False;
                        #</if>
                    #</if>        
                    #By this point, either a decimal point wasn't found in the Volume string, ot one was found, but parsed to be a zero (suggests an integer)
                    #print( "attempting parse to entire Volume string..." )
                    iVolume = int( float(row["volume"]) );
                    row["volume"] = str(iVolume);
#                    if (not sTest == "0000"):
#                        print( "line: ", csv_reader.line_num, " >>> volume not 0000 , is: ", sTest )
#                        isGoodRow = False;

                else:
                   print( "(volume test skipped)" )


                if isGoodRow:
                    writer_good.writerow( row )  #if good after all tests & conversions, write Dictionary row out to cleansed csv file

                if not isGoodRow:
                    writer_bad.writerow(row)  #write Dictionary row out to errors csv file


                line_count += 1

                if isSample:
                    #terminate out of file early (i.e. LIMIT 5)
                    if line_count == 10:
                        break    # break out of loop

            #</for>
        #</with open>
    #</with open>

    #Print summary info
    print(f'Processed {line_count} lines.')

    outputFile.close()
    
