#Import SQL file in Python using pymysql
#Return the data frame.


#Clean up the data listed below to follow the same formats
#Once cleaned, consolidate them together into one dataframe.

#SQL FILE:
#Date, Type, Average price (allowing floats and NA)

#CSV FILE:
#Region, Year, Type


# Data Name : Data Type
# Date : date
# Type : text
# Average price : float
# Region : text
# Year : text


import pymysql as mysql
import getpass
import pandas as pd
import re

username = input("Username: ") #root
password = getpass.getpass("Password: ") #blank



def getSQLData(u, p):
	connection = mysql.connect(host='localhost', user=u, password=p, db='BSCY4', charset='utf8')

	try:
		with connection.cursor() as cursor:
			sql = 'SELECT Type, Date, AveragePrice, Region, Year FROM AVOCADO'
			cursor.execute(sql)

			dfSQL = pd.io.sql.read_sql(sql, connection)

			dfSQL.Date = pd.to_datetime(dfSQL['Date'], format='%Y/%m/%d')


			typeCount = 0
			for ty in dfSQL.Type:
				if(typeCount < len(dfSQL.Type)):
					dfSQL['Type'].replace(ty, ty.lower(), inplace=True)
					typeCount+1


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


			regCount = 0
			for reg in dfSQL.Region:
				if " " in reg:
					dfSQL['Region'].replace(reg, reg.strip(), inplace=True)
					regCount +1

			#return the dataframe
			# return dfSQL

					
	finally:
		connection.close()
				


def getCSVData():
	#parse the data
	dfCSV = pd.read_csv("BSCY4.csv")

	# print(dfCSV.columns)

	
	
	yearCount = 0
	for y in dfCSV.year:
		year = str(int(y))
		if(yearCount < len(dfCSV.year)):
			dfCSV['year'].replace(y, year, inplace=True)
			yearCount+1

	
	dfCSV['type'].replace("Org.", "organic", inplace=True)


	count = 0
	for date, year in zip(dfCSV.Date, dfCSV.year):
		if count < len(dfCSV.Date):
			date = date.replace("-", "/")
			monthDay = re.sub(r'\b[0-9]..[0-9]\b', "", date)
			if monthDay.startswith("/"):
				monthDay = monthDay[1:]
			elif monthDay.endswith("/"):
				monthDay = monthDay[:-1]
			else:
				day = monthDay[:2]
				month = monthDay[-2:]
				monthDay = "%s/%s" % (month, day)

			newDate = "%s/%s" % (year, monthDay)
			# print(newDate)
			dfCSV['Date'].replace(date, str(newDate), inplace=True)
			count+1


	# dfCSV['Date'] = pd.to_datetime(dfCSV['Date'], format='%Y/%m/%d')

	
	print(dfCSV['Date'])	

			

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


	dfCSV.rename(columns={'type': 'Type', 'region': 'Region', 'year':'Year'}, inplace=True)

	df = dfCSV[['Date', 'Year', 'Type', 'Region', 'AveragePrice']]
	return df
	# return dfCSV but only using type, date, year, region, averageprice
	
	#get region
	#get year
	#get type
	#get date - reverse year and day and use /
	#get averageprice 


# def consolidate():
# 	getCSVData().append(getSQLData(username, password), ignore_index=True).to_csv("results.csv", encoding="utf-8", index=False)


# consolidate()


# getSQLData(username, password)
getCSVData()

# print(getCSVData().info())
# print(getSQLData(username, password).info())
