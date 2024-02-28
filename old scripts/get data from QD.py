import socket
import serial
import time
import sys
from datetime import datetime
import ftplib
import glob
import os
import numpy as np


def retrieve_data_from_QD(host, i = 0):
    if i == 0:
        os.chdir(r'C:\Users/Graph/Desktop')
        qd = ftplib.FTP(host, user = 'qd', passwd = '79653')
        # qd.login('qd', '79653')
        qd.cwd('/StorageCard/DAT_Files/')
        entries = list(qd.nlst())
        filename = entries[-1]
        file = open(filename, 'wb')
        qd.retrbinary(f'RETR {filename}', file.write)
        qd.quit()
        
def main():
    i = int(input('Enter the qd number in the following format: if you want to get data from QD1, input 1, if QD2, then 2, and so on. Enter the number here: '))
    if i == 1:
        retrieve_data_from_QD('172.29.32.108')
    if i == 2:
        retrieve_data_from_QD('172.29.32.98')
    if i == 3:
        retrieve_data_from_QD('172.29.32.183')
    print('you can get .dat file at the Desktop')
main()