from os import listdir , makedirs
from os.path import isfile, join, exists, dirname, realpath
import sys
import datetime
import pandas as pd
#import psycopg2
import logging
import configparser
import csv
from mmc.mobilitytrace import MobilityTrace
from mmc.mmc import Mmc
from cluster.djcluster import Djcluster
global OUTPUTPATH
global INPUTPATH
global INPUT
global OUTPUT
global SCRIPT_DIR
global LOGPATH

logging.getLogger().setLevel(logging.INFO)

def buildSubscribersMmc (inputFilePath,outputFolder):
    labels = ["user_id","timestamp","lat","lon"]
    trailmt = dict()
    processedUsers = list()
    pDaysArray=[False,False,False,False,False,False,False,False,False,True]
    pTimeslices =  1

    with open(inputFilePath,'r') as tsvin:
        logging.info("Open: {0}".format(inputFilePath))
        tsvin = csv.reader(tsvin, delimiter=',')
        for row in tsvin:
            idUser = row[labels.index("user_id")]
            latitude = row[labels.index("lat")]
            longitude = row[labels.index("lon")]
            aux_mt = MobilityTrace(
                    row[labels.index("timestamp")],
                    latitude,
                    longitude,
                    "gsm"
                    )
            if ((aux_mt.latitude != 0) & (aux_mt.longitude != 0)):
                if (idUser in trailmt):
                    (trailmt[idUser]).append(aux_mt)
                elif(len(trailmt.items()) == 0):
                    trailmt[idUser] = [aux_mt]
                    if (not idUser in processedUsers):
                        processedUsers.append(idUser)
        logging.info("Executing model")
        minpts = 10
        eps = 10
        key = processedUsers[-1]
        oDjCluster = Djcluster(minpts,eps,trailmt[key])
        #clustering
        oDjCluster.doCluster()
        oDjCluster.post_proccessing()

        #building mobility models
        oMmc = Mmc(oDjCluster,
               trailmt[key],key,
               daysArray=pDaysArray,
               timeSlices=pTimeslices,
               radius=eps
               )
        oMmc.buildModel()
        print (oMmc)
        oMmc.export(outputFolder)

#end buildSubscribersMmc

if __name__ == "__main__":
    result = []
    if len(sys.argv)<=1:
        logging.info("ERROR: You need to specify the path of the config file")
    else:
        cfgName = sys.argv[1]
        config = configparser.ConfigParser()
        config.read(cfgName)
        logging.info("Reading configuration")
        print (config.sections())
        inputFilePath  =  str(config.get('path','inputFilePath'))
        inputFilePath = inputFilePath.replace("\"","")
        logging.info("inputFilePath: {}".format(inputFilePath))
        outputFilePath = config.get('path','outputFilePath')
        experimentName = config.get('experiment','name')
        experimentName = experimentName.replace("\"","")
        outputFilePath =  outputFilePath+experimentName+"/"
        outputFilePath = outputFilePath.replace("\"","")
        logging.info("outputfile: {}".format(outputFilePath))

        logPath =  config.get('experiment','log')
        logPath = logPath.replace("\"","")
        logging.info("locationPath: {}".format(logPath))
        SCRIPT_DIR = dirname(realpath('__file__'))
        OUTPUTPATH =  join(SCRIPT_DIR,outputFilePath)
        INPUTPATH = join(SCRIPT_DIR,inputFilePath)
        MINPTS = int(config.get('parameters','minpts'))
        EPS = float(config.get('parameters','eps'))
        LOGPATH = join(SCRIPT_DIR,"users_{}.txt".format(experimentName))
        logging.info("SCRIPT_DIR: {} ".format(LOGPATH))
        if not exists(outputFilePath):
            makedirs(outputFilePath)
        t_begin =  datetime.datetime.now()
        t_end =  datetime.datetime.now()
        buildSubscribersMmc(INPUTPATH, OUTPUTPATH)
        logging.info("The work end successfully in {} time".format(str(t_end-t_begin)))
