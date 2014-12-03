'''
Created on 23 Oct 2014

@author: dusted-ipro
'''

import matplotlib.pyplot as plt
import csv
import numpy as np

if __name__ == '__main__':

    '''
    "Age","Gender","Impressions","Clicks","Signed_In"
    36,0,3,0,1
    73,1,3,0,1
    30,0,3,0,1
    '''


#Load a csv
fileN = "/Users/dusted-ipro/Downloads/doing_data_science-master/dds_datasets/nyt1.csv"
inData = []
cnt = 0
#Load into a structure
with open(fileN, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in spamreader:
        if cnt!=0:
            out = [int(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4])]
            inData.append(out)
        cnt+=1
print cnt



#First exercise - grouping by age and graphing impressions and clicks
gp18_imp = []
gp18_clicks = []
gp18_24_imp = []
gp18_24_clicks = []
for row in inData:
    if row[0]<18:
        gp18_clicks.append(row[3])
        gp18_imp.append(row[2])
    elif row[0]>=18 and row[0]<=24:
        gp18_24_clicks.append(row[3])
        gp18_24_imp.append(row[2])

plt.plot(gp18_imp, gp18_clicks, 'ro')
#plt.plot(gp18_24_imp, gp18_24_clicks, 'bo')
#plt.axis([min(gp18_imp), max(gp18_imp), min(gp18_clicks), max(gp18_clicks)])
plt.axis([0,25,0,6])
plt.show()

'''
#Second - Clicks by gender bar chart
mClicks = 0
fClicks = 0
for row in inData:
    if row[1]==0:
        mClicks+=row[3]
    else:
        fClicks+=row[3]

fig, ax = plt.subplots()
ax.bar([5,1], [0,1,2,3,4,5,4,3], width=100)
plt.show()


del inData

'''



