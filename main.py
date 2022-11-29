"""
This program takes a single csv file with the correct Census API formatting and gets batch results from API,
splitting files into 10,000 item chunks if needed.

Code Author: Cole Koryto
"""

import os
import censusgeocode as cg
import pandas as pd

# splits csv file into 10,000 line files and returns file names
def split(csvFileName):
    fileLines = open(csvFileName, 'r').readlines()
    fileNames = []
    filename = 1
    for i in range(len(fileLines)):
        if i % 10000 == 0:
            open(str(filename) + '.csv', 'w+').writelines(fileLines[i:i + 1000])
            fileNames.append(str(filename) + '.csv')
            filename += 1

    # adds all rows to 1.csv if no files had to be split off
    if len(fileNames) == 0:
        open('1.csv', 'w+').writelines(fileLines)
        fileNames.append('1.csv')

    # returns split file names
    return fileNames

# takes split csv files names and gets all results
def getBatchResults(splitFileNames):

    # loops through all file names
    overallResultsDataframe = pd.DataFrame()
    for batchNum, fileName in enumerate(splitFileNames):
        print("Getting results for batch " + str(batchNum + 1) + " of " + str(len(splitFileNames)))
        resultDf = pd.DataFrame.from_dict(cg.addressbatch(fileName))

        # sets first result dataframe to be overall and appends all other results
        if overallResultsDataframe.empty:
            overallResultsDataframe = resultDf
        else:
            overallResultsDataframe = pd.concat([overallResultsDataframe, resultDf], axis=0)

    # makes new file name
    newFileName = 'GeocodeResults'

    # looks for open file name adds number to end that represents what number output it is
    openFileName = False
    numChecks = 1
    while not openFileName:
        tempFileName = newFileName + " " + str(numChecks) + ".csv"
        if not os.path.exists(tempFileName):
            newFileName = tempFileName
            break
        numChecks += 1

    # outputs overall results to csv file
    overallResultsDataframe.to_csv(newFileName)

# main functions of the program
def main():

    validFileName = False
    while not validFileName:

        try:
            # gets input file name from user
            inputFileName = input("Please enter the file name of the input file with file extension included\n")

            # splits main csv file and gets new file names
            splitFileNames = split(inputFileName)
            validFileName = True

        except:
            print("Error splitting given file.\n")

    # gets all result for given addresses
    getBatchResults(splitFileNames)

    # removes temp split files
    for fileName in splitFileNames:
        os.remove(fileName)

    # pauses program before closing
    input("\nThe program has completed successfully.\nPress enter to exit.")

# runs main function if file is called as main file
if __name__ == '__main__':
    main()
