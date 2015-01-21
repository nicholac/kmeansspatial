'''
Created on 7 Nov 2014

@author: dusted-ipro

Tries to do some Knn clustering


'''

from pymongo import MongoClient
from random import randint
import numpy as np
from operator import itemgetter
from math import hypot
import matplotlib.pyplot as plt
from scipy.spatial.distance import pdist


def MongoConnTwitter():
    '''Conn to Mongo and return
    '''
    client = MongoClient()
    db = client.twitter
    coll = db.live_tweets
    return coll, client

def knn(inArr, k, maxIters):
    '''Does the knn:
    1) select k random centroids (x,y) - near data but different to one another
    2) assign each test Vector to its closest centroid vector
    3) Find the average location of the test vectors set for each centroid (k) set
    3) Assign the centroids (k) to this average location
    repeat 2 and 3 until the assignments dont change (or change very little)
    We are not selecting new random centroids - we are iterating the find closest...
    ...data points until there is no change
    maxIters ensures we have some break if not finding a good fit
    errorTol is the measure for when we stop - Not used at present
    '''
    #Find the min and max of the data so seed centroids are near the data
    minX = 0
    maxX = 0
    minY = 0
    maxY = 0
    for vec in inArr:
        if minX>vec[0]:
            minX = vec[0]
        if maxX<vec[0]:
            maxX = vec[0]
        if minY>vec[1]:
            minY = vec[1]
        if maxY<vec[1]:
            maxY = vec[1]
    print minX, maxX, minY, maxY
    centroids = []
    seedCentroids = []
    d = {}
    #Record for bad centroids
    poppedCents = 0
    #Now we know the bounds lets select some (k) random centroid vectors (2d)
    #TODO: Check they are sufficiently different
    #using dict - [{'centroid'centroidArr, testVecs:[Array of closest vectors]}, ...]
    for i in range(0, k):
        centroid = np.array((randint(minX, maxX), randint(minY, maxY)))
        seedCentroids.append(centroid)
        d['centroidVec'] = centroid
        d['testVecs'] = []
        centroids.append(d.copy())
    print 'Starting Centroids: ' +str(centroids)
    #Start iterating the movement of these centroids
    iters = 0
    while iters < maxIters:
        for testVec in inArr:
            #find closest centroid for each testVector
            dists = []
            for centroid in centroids:
                #scipy n dimension version
                dist = pdist(np.array((testVec, centroid['centroidVec'])))
                #Hypot 2d version - works
                #dist = hypot(testVec[0]-centroid['centroidVec'][0], testVec[1]-centroid['centroidVec'][1])
                dists.append({'centroidVec':centroid['centroidVec'],'testVec':testVec, 'dist':dist})
            minDist = sorted(dists, key=itemgetter('dist'), reverse=False)[0]
            #print minDist
            #Add this testVec to the centroids set
            #TODO Use item getter here instead?
            for centroid in centroids:
                if np.all(np.equal(centroid['centroidVec'], minDist['centroidVec'])):
                    centroid['testVecs'].append(minDist['testVec'])

        #Now work out the average of the testVec's assigned to each centroid & reset the centroid to this average vector
        #Lets do this very simply - average of the x's and y's
        #TODO: Do this some other smarter way - np.centroid?
        #This is the mount each centroid has moved this iteration
        errs = []
        for idx, centroid in enumerate(centroids):
            #Check to make sure the centroid has some vectors in its set
            #If its too far to have anything in cluster then pop it out of the equation
            if centroid['testVecs']==[]:
                centroids.pop(idx)
                poppedCents+=1
                continue
            #Computing the average of this cluster
            #method 1- numpy - doesnt lower the error
            #avgVec = np.average(centroid['testVecs'], axis=0)

            #Method2 - Compute the average number for both axes - then set this as the vector
            avgX = 0
            avgY = 0
            for vec in centroid['testVecs']:
                avgX+=vec[0]
                avgY+=vec[1]
            #Now divide
            avgX = avgX/len(centroid['testVecs'])
            avgY = avgY/len(centroid['testVecs'])
            avgVec = np.array((avgX, avgY))

            #Record the amount we moved
            #Scipy n dim version
            dist = pdist(np.array((avgVec, centroid['centroidVec'])))
            #Math hypot 2d version - works
            #dist = hypot(avgVec[0]-centroid['centroidVec'][0], avgVec[1]-centroid['centroidVec'][1])
            errs.append(dist)
            #Reset our centroids based on newly calculated average locations
            centroid['centroidVec']=avgVec
            #reset testvecs
            centroid['testVecs']=[]

        #Completed one iteration - go back and select nearest points and move again
        print 'Iterations: '+str(iters)+ ' Errors: '+str(errs)+ ' popped centroids: '+str(poppedCents)
        iters +=1

    #Completed our iterations - now have our clusters in arrays: {centroid:[[words, followers]]}
    #Return the clusters
    return centroids, seedCentroids



def textFollowersMat(mongoColl, limit):
    '''Builds the 2D numpy array of word counts (content) and followers
    '''
    #figure out how many data points we'll have
    outMatrix = []
    #Build array of vectors of content - [content, followers]
    tweets = mongoColl.find().limit(limit)
    m = 0
    for tweet in tweets:
        #Count content amount
        wordCnt = len(tweet['text'].split(" "))
        #Count Followers
        followersCnt = tweet['user']['followers_count']
        if followersCnt>m:
            m = followersCnt
        #Make numpy vector
        vec = np.array((wordCnt, followersCnt))
        outMatrix.append(vec)
    print 'Max Followers: '+str(m)
    #Done! - return out matrix
    client.close()
    return outMatrix


def friendsStatusesMat(mongoColl, limit):
    '''Builds the 2D numpy array of friends counts (content) and statuses
    '''
    #figure out how many data points we'll have
    outMatrix = []
    #Build array of vectors of content - [content, followers]
    tweets = mongoColl.find().limit(limit)
    m = 0
    for tweet in tweets:
        #Count content amount
        friendsCnt = tweet['user']['friends_count']
        #Count Followers
        statuses = tweet['user']['statuses_count']
        #Make numpy vector
        vec = np.array((friendsCnt, statuses))
        outMatrix.append(vec)
    #Done! - return out matrix
    client.close()
    return outMatrix


def makeImage(inArr):
    '''Makes an image from 2d array for viewing data
    '''
    minX = 0
    maxX = 0
    minY = 0
    maxY = 0
    for vec in inArr:
        if minX>vec[0]:
            minX = vec[0]
        if maxX<vec[0]:
            maxX = vec[0]
        if minY>vec[1]:
            minY = vec[1]
        if maxY<vec[1]:
            maxY = vec[1]
    print minX, maxX, minY, maxY
    #Make image array
    imgArr = np.zeros((maxX-minX+1, maxY-minY+1))
    print imgArr.shape
    #Populate
    for vec in inArr:
        imgArr[vec[0], vec[1]]+=1

    return imgArr


if __name__ == '__main__':
    #Get the mongo dataset
    coll, client = MongoConnTwitter()
    #Build the matrix of followers vs content amount - limit amount of tweets processed
    outArr = friendsStatusesMat(coll, 1000)
    #imgArr = makeImage(outArr)
    #Close mongo conn
    client.close()
    #Do our Knn - input array, k and number of iterations
    clusters, seedCentroids = knn(outArr, 10, 5)
    #Do some plotting
    clusterX = []
    clusterY = []
    for c in clusters:
        clusterX.append(c['centroidVec'][0])
        clusterY.append(c['centroidVec'][1])

    seedX = []
    seedY = []
    for d in seedCentroids:
        seedX.append(d[0])
        seedY.append(d[1])

    #fig, axes = plt.subplots(nrows=2)
    #axes[0].imshow(imgArr)

    plt.plot(clusterX, clusterY, 'ro')
    plt.plot(seedX, seedY, 'bo')
    #plt.axis([0,25,0,6])
    plt.show()


    del clusters
    print 'Done'










