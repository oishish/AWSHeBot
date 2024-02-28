import slack_sdk as slack
import os
from pathlib import Path
from dotenv import load_dotenv
from matplotlib import rc
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import time
import glob
import pandas as pd
import mysql.connector as mysql
import socket
import sys
from slack_sdk.errors import SlackApiError

folder_path = r'//LH2MONITORPC/LHe Log/'
file_type = r'\*.txt'


# Functions related to the Autamtic Compressor Reload

def func(x, a=0.41676362, b=1.15092017,
         c=4.23090013):  # conversion factor function. helps predict the conversiion factor from the He Level in sm to the Volume in L, has an errorbar witin 0.27 liters
    return a * np.log(x - c) + b


# functions related to the SlackBot
def importData():
    files = glob.glob(folder_path + file_type)
    iden = max(files, key=os.path.getctime)
    data = np.genfromtxt(iden, delimiter='\t', dtype='str')
    return data


def uploadLastRow(QD1_Volume_data, QD1_Rate_of_Change, QD1_P_dewar, QD1_SP_press, QD2_Volume_data, QD2_Rate_of_Change,
                  QD2_P_dewar, QD2_SP_press, QD3_Volume_data, QD3_Rate_of_Change, QD3_P_dewar, QD3_SP_press,  QD1_connection, QD2_connection, QD3_connection):
    try:
        data = importData()
        values = data
        print('connecting to MySql')
        conn = mysql.connect(host='gator4099.hostgator.com', user='afy2003_LHeBufferBot', passwd='rwnVv3%MXns3j;X{',
                             database='afy2003_LHeMonitor')
        print("connection has been established")
        cursor = conn.cursor()
        now = datetime.now()
        formatted_date = now.strftime('%Y%m%d%H%M%S')
        formatted1 = datetime.strptime(values[-1][0], '%m/%d/%Y').strftime('%Y%m%d')
        formatted2 = datetime.strptime(values[-1][1], '%H:%M:%S %p').strftime('%H%M%S')
        for i in range(values[:, 6].size):
            if values[i, 6] == '> 150':
                values[i, 6] = float(values[i, 5]) * func(float(values[i, 5]))

        try:
            cursor.execute(
                "INSERT INTO Status VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s, %s, %s, %s)",
                (formatted1 + formatted2, formatted1 + formatted2, str(values[-1][2]), str(values[-1][3]),
                 str(values[-1][4]), str(values[-1][5]),
                 str(values[-1][6]), str(values[-1][7]), str(values[-1][8]), str(values[-1][9]), str(values[-1][10]),
                 str(values[-1][11]),
                 str(values[-1][12]), str(values[-1][13]), str(values[-1][14]), str(values[-1][15]),
                 str(values[-1][16]), str(values[-1][17]),
                 str(values[-1][18]), str(values[-1][19]), str(formatted_date), str(QD1_Volume_data),
                 str(QD1_Rate_of_Change), str(QD1_P_dewar), str(QD1_SP_press), str(QD2_Volume_data),
                 str(QD2_Rate_of_Change), str(QD2_P_dewar), str(QD2_SP_press), str(QD1_connection), str(QD2_connection),
                 str(QD3_Volume_data), str(QD3_Rate_of_Change), str(QD3_P_dewar), str(QD3_SP_press), str(QD3_connection)))
            conn.commit()
        except mysql.Error as err:
            print("Something went wrong: {}".format(err))
        conn.close()
    except:
        time.sleep(60)
        uploadLastRow(QD1_Volume_data, QD1_Rate_of_Change, QD1_P_dewar, QD1_SP_press, QD2_Volume_data,
                      QD2_Rate_of_Change, QD2_P_dewar, QD2_SP_press, QD3_Volume_data, QD3_Rate_of_Change, QD3_P_dewar, QD3_SP_press,  QD1_connection, QD2_connection, QD3_connection)


def getTheErrayOfValues(df):
    return df.values


def uploadAllData():
    data = importData()
    values = getTheErrayOfValues(data)
    print('connecting to MySql')
    conn = mysql.connect(host='gator4099.hostgator.com', user='afy2003_LHeBufferBot', passwd='rwnVv3%MXns3j;X{',
                         database='afy2003_LHeMonitor')
    print("connection has been established")
    cursor = conn.cursor()
    x, y = values.shape
    for i in np.arange(-1000, -1, 1):
        cursor.execute("INSERT INTO Status VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                       (str(values[i][0]), str(values[i][1]), values[i][2], values[i][3], values[i][4], values[i][5],
                        values[i][6], values[i][7], values[i][8], values[i][9], values[i][10], values[i][11],
                        values[i][12], values[i][13], values[i][14], values[i][15], values[i][16], values[i][17],
                        values[i][18], values[i][19], time.time()))
        # time.sleep(1)
    print('done uploading')
    conn.commit()
    conn.close()


def plotpicture():  # Send the He data to the chat
    env_path = Path(".") / '.env'
    load_dotenv(dotenv_path=env_path)
    try:
        client = slack.WebClient(
            token='xoxb-30076933861-2694531337910-DJfzrtDE5Qm3VXFOJzKGx0C3')  # xoxp-30076933861-2676256068304-2741126789105-862adf3b175790112a5c82a43c7819a4
        client.files_upload(
            channels="#lhe-data",
            file="8hrGraph.png",
            title="Test upload"
        )
    except SlackApiError as e:
        print("Internet connection lost, or ...{}", format(e))
        time.sleep(60)
        plotpicture()


def informofConnectionLost():
    env_path = Path(".") / '.env'
    load_dotenv(dotenv_path=env_path)
    try:
        client = slack.WebClient(token='xoxb-30076933861-2694531337910-DJfzrtDE5Qm3VXFOJzKGx0C3')
        client.chat_postMessage(channel='#lhe-consumption',
                                text='Failed to connect to QD monitor computer. Check both computers')
    except:
        print("Internet connection lost, Warning Pressure")
        informofConnectionLost()
        time.sleep(10)


def connectionReestablished():
    env_path = Path(".") / '.env'
    load_dotenv(dotenv_path=env_path)
    try:
        client = slack.WebClient(token='xoxb-30076933861-2694531337910-DJfzrtDE5Qm3VXFOJzKGx0C3')
        client.chat_postMessage(channel='#lhe-consumption',
                                text='Connection has been reestablished. Disregard previous messsage.')
    except:
        print("Internet connection lost, Warning Pressure")
        informofConnectionLost()
        time.sleep(10)


def CryomechHighPressureWarning():
    env_path = Path(".") / '.env'
    load_dotenv(dotenv_path=env_path)
    try:
        client = slack.WebClient(token='xoxb-30076933861-2694531337910-DJfzrtDE5Qm3VXFOJzKGx0C3')
        client.chat_postMessage(channel='#lhe-consumption',
                                text='Cryomech pressure is above 12PSI. Check Cryomech Liquifier')
    except:
        print("Internet connection lost, Warning Pressure")
        warinngMessage()
        time.sleep(10)


def connectionToCryomechWarning():
    env_path = Path(".") / '.env'
    load_dotenv(dotenv_path=env_path)
    try:
        client = slack.WebClient(token='xoxb-30076933861-2694531337910-DJfzrtDE5Qm3VXFOJzKGx0C3')
        client.chat_postMessage(channel='#lhe-consumption',
                                text='The connection to cryomech was interrupted. Check cryomech Liquifier')
    except:
        print("Internet connection lost, Warning Pressure")
        warinngMessage()
        time.sleep(10)


def warinngMessage():  # send preassure warning message and automatically reload the compressor
    env_path = Path(".") / '.env'
    load_dotenv(dotenv_path=env_path)
    try:
        client = slack.WebClient(token='xoxb-30076933861-2694531337910-DJfzrtDE5Qm3VXFOJzKGx0C3')
        client.chat_postMessage(channel='#lhe-consumption',
                                text='Pressure is below 300 PSI. Compressor needs to be reloaded!')
    except:
        print("Internet connection lost, Warning Pressure")
        warinngMessage()
        time.sleep(10)


def warningPuritySensor(Pure):  # purity sensor warning message
    env_path = Path(".") / '.env'
    load_dotenv(dotenv_path=env_path)
    try:
        client = slack.WebClient(token='xoxb-30076933861-2694531337910-DJfzrtDE5Qm3VXFOJzKGx0C3')
        # if Pure > 20:
        # client.chat_postMessage(channel='#lhe-consumption', text='Purity sensor reading high. Check on Purifier.')
        if Pure < 0.5:
            client.chat_postMessage(channel='#lhe-consumption',
                                    text='Purity sensor reading low. The Purifier is regenerating.')
    except:
        print("Internet connection lost, Warning Purity")
        time.sleep(60)
        warningPuritySensor(Pure)


def graph(t1, P1):  # make a plot out of the He data
    rc('font', **{'size': 5})
    rc('lines', **{'linewidth': 1.0})
    rc('axes', **{'labelsize': 16})
    rc('xtick', **{'direction': 'in', 'top': True, 'minor.visible': True})
    rc('ytick', **{'direction': 'in', 'right': True, 'minor.visible': True})
    plt.plot(t1, P1)
    plt.ylim(0, 400)
    plt.xlabel('time')
    plt.ylabel('Cylinder Pressure (PSI)')
    plt.savefig('8hrGraph.png')
    plt.cla()
    plt.clf()
    plt.close('all')


def checkconnection(s, i=0):
    try:
        string = 'connection check'
        string = string.encode('utf-8')
        string += b'' * (1024 - len(string))
        s.send(string)
    except:
        if i == 0:
            informofConnectionLost()
            print('connection lost')
            s = establishConnection()
            time.sleep(60)
            s = checkconnection(s, i=1)
            connectionReestablished()
        else:
            s = establishConnection()
            time.sleep(60)
            s = checkconnection(s, i=1)
    return s


def establishConnection():
    try:
        s = socket.socket()
        host = '128.111.16.88'  # insert the ip address of the host computer
        port = 5050
        # ser = serial.Serial(port = 'COM#', baudrate = 9600)#insert port number, in place of '#'
        s.connect((host, port))
        print('Computer has Connected to QD Monitor')
    except:
        time.sleep(3)
    return s


def main():
    t0 = 0
    P = np.zeros((1, 3))
    Pure = np.zeros((1, 3))
    CryoP = np.zeros((1, 3))
    ind = 1
    x = 0
    n = 0
    oldrows = -1
    timeinterval = 3 * 60 * 60  # graph refreshing time in seconds
    begin = datetime.now()
    beginToCheck = begin
    last_purity_warning = datetime.now()
    last_pressure_warning = datetime.now()
    currentdate = begin
    QD1_Volume_data = []
    QD1_Rate_of_Change = []
    QD1_P_dewar = []
    QD1_SP_press = []
    QD2_Volume_data = []
    QD2_Rate_of_Change = []
    QD2_P_dewar = []
    QD2_SP_press = []
    QD3_Volume_data = []
    QD3_Rate_of_Change = []
    QD3_P_dewar = []
    QD3_SP_press = []
    QD1_connection = []
    QD2_connection = []
    QD3_connection = []
    QD_number_of_data = 0
    s = establishConnection()
    try:
        s.settimeout(3)  # break if waiting for data for more than 3 seconds
        data = s.recv(1024)  # acquire data from the QD monitor
        data = str(data, 'utf-8')
        if len(data) > 0:
            print(data)  # print out the data from QD Monitor
            data = data.split()
            QD1_Volume_data.append(data[1])
            QD1_Rate_of_Change.append(data[2])
            QD1_P_dewar.append(data[3])
            QD1_SP_press.append(data[4])
            QD2_Volume_data.append(data[5])
            QD2_Rate_of_Change.append(data[6])
            QD2_P_dewar.append(data[7])
            QD2_SP_press.append(data[8])
            QD3_Volume_data.append(data[9])
            QD3_Rate_of_Change.append(data[10])
            QD3_P_dewar.append(data[11])
            QD3_SP_press.append(data[12])
            QD1_connection.append(data[13])
            QD2_connection.append(data[14])
            QD3_connection.append(data[15])
            QD_number_of_data += 1

    except:
        pass
    while True:
        # Find the last modified file in the data directory
        try:
            files = glob.glob(folder_path + file_type)
            iden = max(files, key=os.path.getctime)

            # Read the data
            data = np.genfromtxt(iden, delimiter='\t', dtype='str')
            rows, cols = data.shape
        except:
            connectionToCryomechWarning()
        if rows != oldrows:
            t = []
            P = np.zeros(rows)
            Pure = np.zeros(rows)
            CryoP = np.zeros(rows)
            for i in range(rows):
                stime = data[i, 0] + " " + data[i, 1]
                current = datetime.strptime(stime, "%m/%d/%Y %I:%M:%S %p")
                t.append(current)
                P[i] = float(data[i, 17])
                Pure[i] = float(data[i, 18])
                CryoP[i] = float(data[i, 2])
            print(P[-1])

            # Check the Medium Pressure Tanks
            if P[-1] < 300 and ind == 0:  # condition to reload the compressor
                string = 'pushTheButton'
                string = string.encode('utf-8')
                string += b'' * (1024 - len(string))
                try:
                    s.send(string)
                except:
                    s = checkconnection(s)
                warinngMessage()
                ind = 1
            if P[-1] > 387:  # threshold maximum pressure
                ind = 0

            # Check the Helium purity sensor
            # Purity sensor is usually ~2 nA, if it is lower the traps are probably regenerating, if it is high check on the purifier (water absorbers likely)
            # If will also drop low is the Medium Pressure tanks are empty, ignore this.
            # Only post one of these messages every 24 hours, otherwise will spam the channel.
            if CryoP[-1] >= 12:
                timedelta = currentdate - last_pressure_warning
                if timedelta.total_seconds() > 24 * 60 * 60:
                    CryomechHighPressureWarning()
                    last_pressure_warning = datetime.now()

            # Plot the data
            t = np.asarray(t)
            currentdate = datetime.now()
            print('Pressure equals', P[-1], 'at', t[-1])
            timedelta = currentdate - begin
            if timedelta.total_seconds() > timeinterval:  # condition to send lhe-data a plot of consumption
                try:
                    graph(t[np.size(t) - 3*1009: -1], P[np.size(t) - 3*1009: -1])  # saves a graph for the helium pressure in the last week
                except:
                    graph(t, P)  # If not a weeks worth of data, plot it all
                plotpicture()
                begin = currentdate
                s = checkconnection(s)

            print(f'Data points received from QD Monitor: {QD_number_of_data}')
            if QD1_Volume_data:
                uploadLastRow(QD1_Volume_data[-1], QD1_Rate_of_Change[-1], QD1_P_dewar[-1], QD1_SP_press[-1], QD2_Volume_data[-1], QD2_Rate_of_Change[-1], QD2_P_dewar[-1], QD2_SP_press[-1], QD3_Volume_data[-1], QD3_Rate_of_Change[-1], QD3_P_dewar[-1], QD3_SP_press[-1],  QD1_connection[-1], QD2_connection[-1], QD3_connection[-1])  # upload new data to MySQL database to update the website.
            else:
                print('no QD data yet')
            oldrows = rows
        try:
            s.settimeout(3)  # break if waiting for data for more than 3 seconds
            data = s.recv(1024)  # acquire data from the QD monitor
            data = str(data, 'utf-8')
            if len(data) > 0:
                print(data)  # print out the data from QD Monitor
                data = data.split()
                QD1_Volume_data.append(data[1])
                QD1_Rate_of_Change.append(data[2])
                QD1_P_dewar.append(data[3])
                QD1_SP_press.append(data[4])
                QD2_Volume_data.append(data[5])
                QD2_Rate_of_Change.append(data[6])
                QD2_P_dewar.append(data[7])
                QD2_SP_press.append(data[8])
                QD3_Volume_data.append(data[9])
                QD3_Rate_of_Change.append(data[10])
                QD3_P_dewar.append(data[11])
                QD3_SP_press.append(data[12])
                QD1_connection.append(data[13])
                QD2_connection.append(data[14])
                QD3_connection.append(data[15])
                QD_number_of_data += 1
                if QD1_connection[-1] == 1:
                    QD1_Volume_data[-1], QD1_Rate_of_Change[-1], QD1_P_dewar[-1], QD1_SP_press[-1] = QD1_Volume_data[-2], QD1_Rate_of_Change[-2], QD1_P_dewar[-2], QD1_SP_press[-2]
                if QD2_connection[-1] == 1:
                    QD2_Volume_data[-1], QD2_Rate_of_Change[-1], QD2_P_dewar[-1], QD2_SP_press[-1] = QD2_Volume_data[-2], QD2_Rate_of_Change[-2], QD2_P_dewar[-2], QD2_SP_press[-2]
                if QD3_connection[-1] == 1:
                    QD3_Volume_data[-1], QD3_Rate_of_Change[-1], QD3_P_dewar[-1], QD3_SP_press[-1] = QD3_Volume_data[-2], QD3_Rate_of_Change[-2], QD3_P_dewar[-2], QD3_SP_press[-2]
                if CryoP[-1] <= 6.5 and QD1_P_dewar[-1] < 9 and QD2_P_dewar[-1] < 9:
                    timedelta = currentdate - last_purity_warning
                    if timedelta.total_seconds() > 24 * 60 * 60:
                        warningPuritySensor(Pure[-1])
                        last_purity_warning = datetime.now()

        except:
            pass
        time.sleep(60)


main()