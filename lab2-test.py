#Test

import pymysql as mysql
import getpass
import pandas as pd
import re

# username = input("Username: ") #root
# password = getpass.getpass("Password: ") #blank

def getSQLData(u, p):
	connection = mysql.connect(host='localhost', user=u, password=p, db='BSCY4', charset='utf8')

	try:
		with connection.cursor() as cursor:
			sql = 'SELECT Type, Date, AveragePrice, Region, Year FROM AVOCADO'
			cursor.execute(sql)
			dfSQL = pd.io.sql.read_sql(sql, connection)

			#Year
			# print(dfSQL['Year'].unique()) #Finding out what are different
			yearCount = 0
			for year in dfSQL.Year:
				yearAsString = str(year)
				if(yearCount < len(dfSQL.Year)):
					if(len(yearAsString) == 2):
						yearClean = "20%s" % yearAsString
						dfSQL['Year'].replace(year, yearClean, inplace=True)
						yearCount+1
					else:
						dfSQL['Year'].replace(year, yearAsString, inplace=True)

			# pd.to_numeric(dfSQL['Year'], errors='coerce')
			dfSQL['Year'] = dfSQL['Year'].astype('int64')


			#Region
			# print(dfSQL['Region'].unique())
			for i in dfSQL['Region']:
				if re.findall(r"\W", i):
					dfSQL['Region'].replace(i, re.sub('[, ]',"", i), inplace=True)


			#AveragePrice
			dfSQL['AveragePrice'].astype('float64')

			#Type
			# print(dfSQL['Type'].unique()) 
			dfSQL['Type'] = dfSQL['Type'].str.lower()

			#Date
			dfSQL.Date = pd.to_datetime(dfSQL['Date'], format='%Y/%m/%d')

	finally:
		connection.close()

def getCSVData():
	dfCSV = pd.read_csv("BSCY4.csv")
	dfCSV.rename(columns={'type': 'Type', 'region': 'Region', 'year':'Year'}, inplace=True)

	#Year
	dfCSV['Year'] = dfCSV['Year'].astype('int64')	

	#Type
	# print(dfCSV['Type'].unique()) 
	for ty in dfCSV['Type']:
				if re.findall(r"\W", ty):
					dfCSV['Type'].replace(ty, re.sub('Org.',"organic", ty), inplace=True)




	# print(dfCSV['AveragePrice'].unique()) 
	
	avgCount = 0
	for avg in dfCSV.AveragePrice:
		if(avgCount < len(dfCSV.AveragePrice)):
			if "," in str(avg):
				correctAvg =  float(str(avg).replace(",", "."))
				dfCSV['AveragePrice'].replace(avg, correctAvg, inplace=True)
				avgCount+1
			else:

				dfCSV['AveragePrice'].replace(avg, float(avg), inplace=True)
				avgCount+1

	dfCSV['AveragePrice'] = dfCSV['AveragePrice'].astype('float64')	




	#Date - find out what different kinds of entries here -? make changes to cater for each kind -> replace it.
	# for a in dfCSV['Date']:
	# 	print(a)

getCSVData()
# getSQLData("root", "password")