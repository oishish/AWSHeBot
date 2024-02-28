import socket
import serial
import time
import sys
from datetime import datetime
import ftplib
import glob
import os
import numpy as np

#Functions related to the Autamtic Compressor Reload
# Create a Socket ( connect two computers)

def retrieve_data_from_QD(host, i = 0):
    try:
        qd = ftplib.FTP(host, user = 'qd', passwd = '79653')
        # qd.login('qd', '79653')
        qd.cwd('/StorageCard/DAT_Files/')
        entries = list(qd.nlst())
        filename = entries[-1]
        file = open(filename, 'wb')
        qd.retrbinary(f'RETR {filename}', file.write)
        qd.quit()
        folder_path = r'C:\Users/Graph/'
        file_type = r'\*.dat'
        files = glob.glob(folder_path + file_type)
        iden = max(files, key=os.path.getctime)
        data = gendata(iden, i = 7)
        try:
            Volume_data = data[:,10].ravel()
            Rate_of_Change = data[:, 5].ravel()
            P_dewar = data[:, 4].ravel()
            SP_press = data[:, 4].ravel()
        except:
            time.sleep(5*60)
            return retrieve_data_from_QD(host)
        return Volume_data, Rate_of_Change, P_dewar, SP_press, np.asarray([0])
    except ftplib.all_errors as e:
        if i == 1:
            return np.asarray([0]), np.asarray([0]), np.asarray([0]), np.asarray([0]), np.asarray([1])
        print('Error retrieveing the data: ', host, e)
        time.sleep(8*60)
        return retrieve_data_from_QD(host, i = 1)

def gendata(iden, i):
    try:
        return np.genfromtxt(iden, delimiter = ',', usecols=(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24), skip_header = i, skip_footer = 1)
    except Exception as error:
        print(error)
        return gendata(iden, i+1)


def send_data_to_client(data):
    pass


def create_socket():
    try:
        global host
        global port
        global s
        host = '128.111.16.88' #insert public ip adress of the host computer
        print(host)
        port = 5050
        s = socket.socket()
        return s

    except socket.error as msg:
        print("Socket creation error: " + str(msg))


# Binding the socket and listening for connections
def bind_socket():
    try:
        global host
        global port
        global s
        print("Binding the Port: " + str(port))
        s.bind((host, port))
        s.listen(1)

    except socket.error as msg:
        print("Socket Binding error" + str(msg) + "\n" + "Retrying...")
        bind_socket()
        time.sleep(10)


# Establish connection with a client (socket must be listening)

def socket_accept():
    conn, address = s.accept()
    print("Connection has been established! |" + " IP " + address[0] + " | Port" + str(address[1]))
    return conn, address

# Send commands to client/victim or a friend
def send_commands(conn, cmd = 'checking connection'):
    conn.send(str.encode(cmd))
    #client_response = str(conn.recv(1024),"utf-8")
    #print(client_response+'\n', end="")

def close_connection(conn):
    conn.close()
    s.close()

def checkconnection(conn):
    try:
        send_commands(conn)
    except:
        time.sleep(3)
        print('connection is lost.. relaunching the server')
        close_connection(conn)
        create_socket()
        bind_socket()
        print('server relaunched... waiting for connection')
        conn, address = socket_accept()
        return conn
    return conn


def main():
    s = create_socket()
    bind_socket()
    print("Attempting Socket Accept")
    conn, address = socket_accept()
    t = datetime.now()
    print("Setup Complete, attempting to get data")
    Volume_data1211, Rate_of_Change1211, P_dewar1211, SP_press1211, connection1211 = retrieve_data_from_QD(host = '172.29.32.98')
    Volume_data1322, Rate_of_Change1322, P_dewar1322, SP_press1322, connection1322 = retrieve_data_from_QD(host = '172.29.32.108')
    Volume_data1213, Rate_of_Change1213, P_dewar1213, SP_press1213, connection1213 = retrieve_data_from_QD(host = '172.29.32.183')
    try:
        send_commands(conn, cmd = f'QD_data {Volume_data1211[-1]} {Rate_of_Change1211[-1]} {P_dewar1211[-1]} {SP_press1211[-1]} {Volume_data1322[-1]} {Rate_of_Change1322[-1]} {P_dewar1322[-1]} {SP_press1322[-1]} {Volume_data1213[-1]} {Rate_of_Change1213[-1]} {P_dewar1213[-1]} {SP_press1213[-1]} {connection1211[-1]} {connection1322[-1]} {connection1213[-1]}')
    except:
        conn = checkconnection(conn)
        send_commands(conn, cmd = f'QD_data {Volume_data1211[-1]} {Rate_of_Change1211[-1]} {P_dewar1211[-1]} {SP_press1211[-1]} {Volume_data1322[-1]} {Rate_of_Change1322[-1]} {P_dewar1322[-1]} {SP_press1322[-1]} {Volume_data1213[-1]} {Rate_of_Change1213[-1]} {P_dewar1213[-1]} {SP_press1213[-1]} {connection1211[-1]} {connection1322[-1]} {connection1213[-1]}')
    print('first retrieve successful')
    while True:
        try:
            print('trying to read from LHeLog')
            conn.settimeout(0)
            data = str(conn.recv(1024), 'utf-8')
            if len(data) > 0:
                if data == 'pushTheButton':
                    ser = serial.Serial(port = 'COM3', baudrate = 9600)#insert port number, in place of '#'
                    ser.write(str.encode('reload the compressor'))
                    ser.close()
                    pass
                if data == 'connection check':
                    print(datetime.now(), 'connection is on')
                    pass
                print(data)
        except:
            pass
        currentTime = datetime.now()
        timedelta = currentTime - t
        print(timedelta.total_seconds())
        if timedelta.total_seconds() >= 60:
            t = currentTime
            Volume_data1211, Rate_of_Change1211, P_dewar1211, SP_press1211, connection1211 = retrieve_data_from_QD(host= '172.29.32.98')
            Volume_data1322, Rate_of_Change1322, P_dewar1322, SP_press1322, connection1322 = retrieve_data_from_QD(host= '172.29.32.108')
            Volume_data1213, Rate_of_Change1213, P_dewar1213, SP_press1213, connection1213 = retrieve_data_from_QD(host = '172.29.32.183')
            print('Data Successfully retrieved from QDs')
            print(f'QD_data {Volume_data1211[-1]} {Rate_of_Change1211[-1]} {P_dewar1211[-1]} {SP_press1211[-1]} {Volume_data1322[-1]} {Rate_of_Change1322[-1]} {P_dewar1322[-1]} {SP_press1322[-1]} {Volume_data1213[-1]} {Rate_of_Change1213[-1]} {P_dewar1213[-1]} {SP_press1213[-1]} {connection1211[-1]} {connection1322[-1]} {connection1213[-1]}')
            try:
                send_commands(conn, cmd = f'QD_data {Volume_data1211[-1]} {Rate_of_Change1211[-1]} {P_dewar1211[-1]} {SP_press1211[-1]} {Volume_data1322[-1]} {Rate_of_Change1322[-1]} {P_dewar1322[-1]} {SP_press1322[-1]} {Volume_data1213[-1]} {Rate_of_Change1213[-1]} {P_dewar1213[-1]} {SP_press1213[-1]} {connection1211[-1]} {connection1322[-1]} {connection1213[-1]}')
            except:
                conn = checkconnection(conn)
                send_commands(conn, cmd = f'QD_data {Volume_data1211[-1]} {Rate_of_Change1211[-1]} {P_dewar1211[-1]} {SP_press1211[-1]} {Volume_data1322[-1]} {Rate_of_Change1322[-1]} {P_dewar1322[-1]} {SP_press1322[-1]} {Volume_data1213[-1]} {Rate_of_Change1213[-1]} {P_dewar1213[-1]} {SP_press1213[-1]} {connection1211[-1]} {connection1322[-1]} {connection1213[-1]}')
        time.sleep(60)





main()
