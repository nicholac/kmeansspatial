'''
Created on 10 Nov 2014

@author: dusted-ipro
'''


import numpy as np
from sklearn import datasets, linear_model
import csv
import matplotlib.pyplot as plt


def testData():
    '''Just laod in some test data and print it out
    '''
    iris = datasets.load_iris()
    iris_X = iris.data
    #print iris_X
    iris_y = iris.target
    #print iris_y
    diabetes = datasets.load_diabetes()
    diabetes_X_train = diabetes.data[:-20]
    print diabetes_X_train.shape
    diabetes_y_train = diabetes.target[:-20]
    print diabetes_y_train.shape
    del iris

    return

def importManhatten():
    '''Imports the manhatten csv into a numpy array ready for scikit
    '''
    inX = []
    inY = []
    #These are duplicates for matplotlib - just being lazy
    plotY = []
    plotX = []

    #UNique checking for a column - works
    unique = []

    #blg class map
    taxClassMap = {'':None, '4':4, '1':1, '1C':1.75, '2B':2.5, '2':2, '2C':2.75, '2A':2.25, '1A':1.25}

    #Remember Y is the value to predict - X are the features
    fp = open('/Users/dusted-ipro/Downloads/dds_datasets/rollingsales_manhattan.csv', 'rb')
    d = csv.DictReader(fp)
    for row in d:
        #unique.append(row['BUILDING CLASS AT TIME OF SALE'])  #TODO - next make this into a feature we can try out
        blgClass = normBlgClass(row['BUILDING CLASS AT TIME OF SALE'])
        price = int(row['SALE\nPRICE'].replace("$", "").replace(",", ""))
        sqFt = int(row['GROSS SQUARE FEET'].replace(',',''))
        taxCls = taxClassMap[row['TAX CLASS AT PRESENT'].strip()]
        if row['YEAR BUILT'] != '0' and price != 0 and taxCls != None and sqFt != 0 and blgClass != 0:
            inY.append(price)
            #Poor relationships with yr built
            #inX.append([int(row['YEAR BUILT']), sqFt])
            inX.append([blgClass, sqFt])
            plotX.append(int(row['YEAR BUILT']))
            plotY.append(price)
    #Change it to numpy
    outX = np.array(inX)
    outY = np.array(inY)
    fp.close()
    #Unique check
    #getUnique(unique)

    return outX, outY, plotX, plotY


def normBlgClass(inClass):
    '''Normalises building class codes
    '''
    inClass = inClass.strip()
    outClass = ord(inClass[0])+(ord(inClass[1])/10)
    return outClass


def plotManhattan(inX, inY):
    '''Plots the manhattan data on a x,y graph
    '''
    plt.plot(inX, inY, 'ro')
    #plt.axis([0,25,0,6])
    plt.show()
    return

def plotHist(column):
    '''Plots a distribution
    column is the name of the column
    '''
    fp = open('/Users/dusted-ipro/Downloads/dds_datasets/rollingsales_manhattan.csv', 'rb')
    d = csv.DictReader(fp)
    unique = []
    #Run through and get the unique vals
    for row in d:
        unique.append(row[column])
    fp.close()
    bins = getUnique(unique)
    histDict = {}
    fp = open('/Users/dusted-ipro/Downloads/dds_datasets/rollingsales_manhattan.csv', 'rb')
    d = csv.DictReader(fp)
    #Now get counts for these bins
    print column
    for row in d:
        if row[column] in histDict.keys():
            histDict[row[column]]+=1
        else:
            histDict[row[column]]=1
    #Pivot for matplotlib
    data = []
    i=0
    for k in histDict.keys():
        data.append(int(histDict[k]))
    #Bins
    bins = np.arange(len(data))
    fig, ax = plt.subplots()
    rects1 = ax.bar(bins, data, 0.35, color='r')
    plt.xlabel(column)
    #plt.bar(len(bins), data, 0.35)
    #plt.xticks(len(bins) + 0.35, bins)
    plt.show()
    fp.close()
    return


def fuckyoucharts():
    N = 5
    menMeans   = (20, 35, 30, 35, 27)
    womenMeans = (25, 32, 34, 20, 25)
    menStd     = (2, 3, 4, 1, 2)
    womenStd   = (3, 5, 2, 3, 3)
    ind = np.arange(N)    # the x locations for the groups
    width = 0.35       # the width of the bars: can also be len(x) sequence

    p1 = plt.bar(ind, menMeans,   width, color='r', yerr=womenStd)
    p2 = plt.bar(ind, womenMeans, width, color='y',
                 bottom=menMeans, yerr=menStd)

    plt.ylabel('Scores')
    plt.title('Scores by group and gender')
    plt.xticks(ind+width/2., ('G1', 'G2', 'G3', 'G4', 'G5') )
    plt.yticks(np.arange(0,81,10))
    plt.legend( (p1[0], p2[0]), ('Men', 'Women') )

    plt.show()
    return


def getUnique(inData):
    '''Gets unique data in a row
    '''
    out = []
    for i in inData:
        if i.strip() not in out:
            out.append(i.strip())
    print 'UNique values in Col: '
    return out


if __name__ == '__main__':
    #getData
    testData()
    #Cehck a columns unique values
    inX, inY, plotX, plotY = importManhatten()
    #PLot two variables against each other
    plotManhattan(plotX, plotY)

    #PLot a distribution of a single variable
    #plotHist(r'BUILDING CLASS AT TIME OF SALE')
    #fuckyoucharts()
    '''
    #Fire it at scikit - Works
    regr = linear_model.LinearRegression()
    regr.fit(inX, inY)
    #Print coefs
    print(regr.coef_)
    #Get mean squared error
    print 'mean sq err:'
    mse = np.mean((regr.predict(inX)-inY)**2)
    print mse
    #Explained variance score: 1 is perfect prediction
    # and 0 means that there is no linear relationship
    # between X and Y.
    print 'variance score: '
    var = regr.score(inX, inY)
    print var
    '''
    del inX, inY








