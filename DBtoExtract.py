import sys
from tableausdk import *
from tableausdk.Extract import *
import pandas as pd
import mysql.connector
import time
import locale

print "data integration starting..."

#Start stopwatch
startTime = time.clock()

#Date and Time stamp for output file
dateTimeStamp = time.strftime("%Y%m%d_%H%M")
#fileName
output_CSV = dateTimeStamp + '_' + 'DBtoExtract' + ".csv"
output_Extract = dateTimeStamp + '_' + 'DBtoExtract' + ".tde"

#------------------------------------------------------------------------------
#   Create DB Connection
#------------------------------------------------------------------------------

#create connection
cnx = mysql.connector.connect(user='tableau', password='TabHealth321', host='t4hdb.cwc6a68e71l7.us-east-1.rds.amazonaws.com', database='healthcare')

#read tables into dataframes
df_fact = pd.read_sql('SELECT * FROM claims_claims_f', con=cnx)
df_dept = pd.read_sql('SELECT * FROM claims_departments_d', con=cnx)
df_facil = pd.read_sql('SELECT * FROM claims_facilities_d', con=cnx)
df_pat = pd.read_sql('SELECT * FROM claims_patients_d', con=cnx)

#------------------------------------------------------------------------------
#   Create joined Dataframe
#------------------------------------------------------------------------------

#create list of tables
join_info = [[df_dept,'Department_ID'], [df_facil,'Facility_ID'], [df_pat,'Patient_ID']]

#auto inner join dataframes
joinDF_auto = df_fact
for i in join_info:
    #joinedDF = pd.merge(outputDF, VPS_DF, on='IntubationCycle', how='left')
    joinDF_auto = pd.merge(joinDF_auto, i[0], on=i[1], how='inner')

#output to CSV 
joinDF_auto.to_csv(output_CSV, index = False, encoding='utf-8')


#typecast to date format
joinDF_auto['ClaimDate'] = pd.to_datetime(joinDF_auto['ClaimDate'])


#------------------------------------------------------------------------------
#   Create Extract
#------------------------------------------------------------------------------
ExtractAPI.initialize()

new_extract = Extract(output_Extract)

# Create a new table definition with 3 columns
table_definition = TableDefinition()
table_definition.addColumn('Patient_ID', Type.INTEGER)        		# column 0
table_definition.addColumn('ClaimDate', Type.DATE)           		# column 1
table_definition.addColumn('Patient Name', Type.UNICODE_STRING) 	# column 2


#table_definition.addColumn('Claim_ID', Type.INTEGER)        		# column 3
#table_definition.addColumn('Discount', Type.DOUBLE) 				# column 4

#------------------------------------------------------------------------------
#   Populate Extract
#------------------------------------------------------------------------------

new_table = new_extract.addTable('Extract', table_definition)
# Create new row
new_row = Row(table_definition)   # Pass the table definition to the constructor

#loop thru all rows in dataframe
for n in range (0, joinDF_auto.shape[0]):
    
        #printing (temporarily) just for debug
        print("row:",n, "column:0", "value:",joinDF_auto.iloc[n,0])
        print("row:",n, "column:1", "value:",joinDF_auto.iloc[n,1].year,joinDF_auto.iloc[n,1].month,joinDF_auto.iloc[n,1].day)
        print("row:",n, "column:2", "value:",joinDF_auto.loc[n,'Patient Name'])
        
        #print("row:",n, "column:3", "value:",joinDF_auto.iloc[n,3])
        #print("row:",n, "column:4", "value:",joinDF_auto.iloc[n,4])

        new_row.setInteger(0, joinDF_auto.iloc[n,0])
        new_row.setDate(1, joinDF_auto.iloc[n,1].year,joinDF_auto.iloc[n,1].month,joinDF_auto.iloc[n,1].day)
        new_row.setString(2, joinDF_auto.loc[n,'Patient Name'])
        #parse out and set date parts: year, month, day
        
        #new_row.setString(3, joinDF_auto.iloc[n,3])
        #new_row.setString(4, joinDF_auto.iloc[n,4])
        
        new_table.insert(new_row) # Add the new row to the table
        
        
# Close the extract in order to save the .tde file and clean up resources
new_extract.close()
ExtractAPI.cleanup()    

print("------------------------------------------------")
print("------------------------------------------------")
print "data prep finsished..."

# Output elapsed time
print "Elapsed:", locale.format("%.2f", time.clock() - startTime), "seconds"
print("------------------------------------------------")
print("------------------------------------------------")





