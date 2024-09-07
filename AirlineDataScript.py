import pandas as pd
import os
import sqlite3
import warnings
warnings.simplefilter('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

class database:
    def __init__(self, databaseFile):
        self.con = sqlite3.connect(databaseFile)
        self.cur = self.con.cursor()
        print('Connection and cursor open')
        self.tableNames = ['Flights']
        self.dataPresent = self.checkDataPresent()
        print('Data present upon opening is', self.dataPresent)
        
        
    def checkDataPresent(self):
        dataPresent = {}
        
        for tableName in self.tableNames:
            try:
                dataPresent[tableName] = self.cur.execute("SELECT DISTINCT Year, Month FROM " + tableName).fetchall()
            except:
                print('No data present in', tableName)
                dataPresent[tableName] = []
        
        return dataPresent
    
    def addData(self, tableName, filesToAdd, overwrite = False):
        
        for csvToAdd in filesToAdd:
            
            print('Current file is', csvToAdd)
        
            csvToAddSplit = csvToAdd.split('_')
        
            monthToAdd = int(csvToAddSplit[-1][:-4])
            yearToAdd = int(csvToAddSplit[-2])
        
            print('Checking if month, year of CSV is present in data:', (yearToAdd, monthToAdd))
            if (yearToAdd, monthToAdd) in self.dataPresent[tableName]:
                
                print((yearToAdd,monthToAdd), 'present in data')
                
                if overwrite == False:
                    
                    print('overwrite is false, continuing')
                    
                    continue
                
                
                else:
                    
                    print('overwrite is true, removing overlapping entries')
                    
                    self.cur.execute(f"DELETE FROM {tableName} WHERE Year = '{yearToAdd}' AND Month = '{monthToAdd}'")
                    
                    self.dataPresent = self.checkDataPresent()
                    print('dataPresent is now', self.dataPresent)
            else:
                
                print('month, year of CSV not present in data')
                               
            # Check which columns are actually worth loading into the database
            # (i.e. no null columns)
            columnsToUse = self.getCols(filesToAdd)
            
            airlineDF = pd.read_csv(csvToAdd, dtype = {'Div2Airport': 'object', 'Div1Airport': 'object', 
                                                'Div1TailNum': 'object', 'Div2TailNum': 'object', 
                                                'Div3Airport': 'object', 'CancellationCode': 'category',
                                                'Div3TailNum': 'object'}, usecols = columnsToUse, encoding='latin-1')
            
            airlineDF.to_sql(tableName, self.con, if_exists = 'append', chunksize = 100000)
            
            print('Data added')
            
            self.dataPresent[tableName].append((yearToAdd, monthToAdd))
            
            print('dataPresent is now', self.dataPresent)
        
    # Find any columns that are completely null and ensure they don't get put in the database,
    # slowing down performance by taking up space for no reason
    def getCols(self, filesToCheck):
        
        print('Enter getCols')
        
        columns = ['Year','Quarter','Month','DayofMonth','DayOfWeek','FlightDate','Reporting_Airline','DOT_ID_Reporting_Airline',
                   'IATA_CODE_Reporting_Airline','Tail_Number','Flight_Number_Reporting_Airline','OriginAirportID','OriginAirportSeqID',
                   'OriginCityMarketID','Origin','OriginCityName','OriginState','OriginStateFips','OriginStateName','OriginWac',
                   'DestAirportID','DestAirportSeqID','DestCityMarketID','Dest','DestCityName','DestState','DestStateFips','DestStateName',
                   'DestWac','CRSDepTime','DepTime','DepDelay','DepDelayMinutes','DepDel15','DepartureDelayGroups','DepTimeBlk','TaxiOut',
                   'WheelsOff','WheelsOn','TaxiIn','CRSArrTime','ArrTime','ArrDelay','ArrDelayMinutes','ArrDel15','ArrivalDelayGroups',
                   'ArrTimeBlk','Cancelled','CancellationCode','Diverted','CRSElapsedTime','ActualElapsedTime','AirTime','Flights',
                   'Distance','DistanceGroup','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay','FirstDepTime',
                   'TotalAddGTime','LongestAddGTime','DivAirportLandings','DivReachedDest','DivActualElapsedTime','DivArrDelay','DivDistance',
                   'Div1Airport','Div1AirportID','Div1AirportSeqID','Div1WheelsOn','Div1TotalGTime','Div1LongestGTime','Div1WheelsOff',
                   'Div1TailNum','Div2Airport','Div2AirportID','Div2AirportSeqID','Div2WheelsOn','Div2TotalGTime','Div2LongestGTime',
                   'Div2WheelsOff','Div2TailNum','Div3Airport','Div3AirportID','Div3AirportSeqID','Div3WheelsOn','Div3TotalGTime','Div3LongestGTime',
                   'Div3WheelsOff','Div3TailNum','Div4Airport','Div4AirportID','Div4AirportSeqID','Div4WheelsOn','Div4TotalGTime','Div4LongestGTime',
                   'Div4WheelsOff','Div4TailNum','Div5Airport','Div5AirportID','Div5AirportSeqID','Div5WheelsOn','Div5TotalGTime','Div5LongestGTime',
                   'Div5WheelsOff','Div5TailNum','Unnamed: 109']
        
        # Colums I don't want, as they seem irrelevant to me
        columnsToIgnore = ['FlightDate', 'Flights', 'Quarter', 'DOT_ID_Reporting_Airline', 'IATA_CODE_Reporting_Airline',
                           'DepDelayMinutes', 'ArrDelayMinutes', 'DepDel15', 'ArrDel15', 'DepartureDelayGroups',
                           'ArrivalDelayGroups', 'DepTimeBlk', 'ArrTimeBlk', 'DistanceGroup', 'OriginCityMarketID',
                           'DestCityMarketID', 'OriginStateFips', 'DestStateFips', 'OriginWac', 'DestWac']

        columnsToCheck = list(set(columns) - set(columnsToIgnore))
        
        columnsToUse = []
        
        # filesToCheck should include files corresponding to data already in the
        # dataset. Maybe there's a month of data already loaded that had a Div4
        # while the data currently being loaded in only goes to Div3.
        
        for file in filesToCheck:
            print('Checking columns in file', file)
            checkAirlineDF = pd.read_csv(file, usecols = columnsToCheck, encoding='latin-1')
            nonNullColumns = checkAirlineDF.columns[~checkAirlineDF.isnull().all()]
            
            for columnToMove in nonNullColumns:
                columnsToCheck.remove(columnToMove)
                columnsToUse.append(columnToMove)
                
        print('Null columns:', columnsToCheck)

        return columnsToUse
    
    def closeConn(self):
        self.cur.close()
        self.con.close()


airlineDB = database('airline.db')
filesToAdd = []
# Requires data directory in same directory as script.
# Data directory has BTS CSV files split into subdirectories by year
dataFolder = r'AirlineData'
for directory in os.listdir(dataFolder):
    for file in os.listdir(dataFolder + '\\' + directory):
        filesToAdd.append(dataFolder + '\\' + directory + '\\' + file)
        
airlineDB.addData('Flights', filesToAdd, overwrite = False)

airlineDB.closeConn()