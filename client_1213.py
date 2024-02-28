import boto3
import time
import time
from datetime import datetime
import ftplib
import glob
import os
import numpy as np

DB_NAME = "LHe-Logging"
ACCESS_ID = "AKIASS4P3NHAP7FRYIXV"
ACCESS_KEY = "J3FlheFBEDOI4zS0eFRSwQP/nj/9LHkw9tSw6V4U"

QD_DICT = {
  "QD1": '172.29.32.108',
  "QD2": '172.29.32.98',
  "QD3": '172.29.32.183'
}

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
    ## If there is no connection, this QD likely disconnected. Report zeros
    except ftplib.all_errors as e:
        print('Error retrieving the data: ', host, e)
        return np.asarray([0]), np.asarray([0]), np.asarray([0]), np.asarray([0]), np.asarray([1])

def gendata(iden, i):
    try:
        return np.genfromtxt(iden, delimiter = ',', usecols=(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24), skip_header = i, skip_footer = 1)
    except Exception as error:
        print(error)
        return gendata(iden, i+1)
    
def main():

    ## Initially, retrieve data from each of the three quantum designs

    for QD in QD_DICT.keys():
        Volume_data, Rate_of_Change, P_dewar, SP_press, Connection_state = retrieve_data_from_QD(host = QD_DICT[QD])
        
        CURRENT_TIME = str(int(time.time() * 1000))
        print(CURRENT_TIME)

        client = boto3.client('timestream-write', region_name='us-east-2', aws_access_key_id=ACCESS_ID, aws_secret_access_key= ACCESS_KEY)

        dimension1 = [ {'Name': 'Parameter', 'Value': 'Volume'}, {'Name': 'Units', 'Value': 'Liters'}, {'Name': 'Quantum Design Liquifier', 'Value': QD}]
        dimension2 = [ {'Name': 'Parameter', 'Value': 'Rate'}, {'Name': 'Units', 'Value': 'Liters/day'}, {'Name': 'Quantum Design Liquifier', 'Value': QD}]
        dimension3 = [ {'Name': 'Parameter', 'Value': 'Pressure Actual'}, {'Name': 'Units', 'Value': 'PSI'}, {'Name': 'Quantum Design Liquifier', 'Value': QD}]
        dimension4 = [ {'Name': 'Parameter', 'Value': 'Pressure Setpoint'}, {'Name': 'Units', 'Value': 'PSI'}, {'Name': 'Quantum Design Liquifier', 'Value': QD}]
        dimension5 = [ {'Name': 'Parameter', 'Value': 'Connection status'}, {'Name': 'Units', 'Value': 'Boolean'}, {'Name': 'Quantum Design Liquifier', 'Value': QD}]
        print(str(Volume_data), str(Rate_of_Change), str(P_dewar), str(SP_press), str(Connection_state))
        record1 = { 'Time': CURRENT_TIME, 'Dimensions': dimension1, 'MeasureName': 'Volume', 'MeasureValue': str(Volume_data),'MeasureValueType': 'DOUBLE'}
        record2 = { 'Time': CURRENT_TIME, 'Dimensions': dimension2, 'MeasureName': 'Rate', 'MeasureValue': str(Rate_of_Change),'MeasureValueType': 'DOUBLE'}
        record3 = { 'Time': CURRENT_TIME, 'Dimensions': dimension3, 'MeasureName': 'Pactual', 'MeasureValue': str(P_dewar),'MeasureValueType': 'DOUBLE'}
        record4 = { 'Time': CURRENT_TIME, 'Dimensions': dimension4, 'MeasureName': 'Psetpoint', 'MeasureValue': str(SP_press),'MeasureValueType': 'DOUBLE'}
        record4 = { 'Time': CURRENT_TIME, 'Dimensions': dimension5, 'MeasureName': 'Connection', 'MeasureValue': str(Connection_state),'MeasureValueType': 'DOUBLE'}

        records = [record1,record2,record3,record4]

        response = client.write_records(DatabaseName=DB_NAME,TableName=QD,Records=records)

        print(response)




    while True:
        currentTime = datetime.now()
        timedelta = currentTime - t
        print(timedelta.total_seconds())
        if timedelta.total_seconds() >= 60:
            t = currentTime
            for QD in QD_DICT.keys():
                Volume_data, Rate_of_Change, P_dewar, SP_press, Connection_state = retrieve_data_from_QD(host = QD_DICT[QD])
                
                CURRENT_TIME = str(int(time.time() * 1000))
                print(CURRENT_TIME)

                client = boto3.client('timestream-write', region_name='us-east-2', aws_access_key_id=ACCESS_ID, aws_secret_access_key= ACCESS_KEY)

                dimension1 = [ {'Name': 'Parameter', 'Value': 'Volume'}, {'Name': 'Units', 'Value': 'Liters'}, {'Name': 'Quantum Design Liquifier', 'Value': QD}]
                dimension2 = [ {'Name': 'Parameter', 'Value': 'Rate'}, {'Name': 'Units', 'Value': 'Liters/day'}, {'Name': 'Quantum Design Liquifier', 'Value': QD}]
                dimension3 = [ {'Name': 'Parameter', 'Value': 'Pressure Actual'}, {'Name': 'Units', 'Value': 'PSI'}, {'Name': 'Quantum Design Liquifier', 'Value': QD}]
                dimension4 = [ {'Name': 'Parameter', 'Value': 'Pressure Setpoint'}, {'Name': 'Units', 'Value': 'PSI'}, {'Name': 'Quantum Design Liquifier', 'Value': QD}]
                dimension5 = [ {'Name': 'Parameter', 'Value': 'Connection status'}, {'Name': 'Units', 'Value': 'Boolean'}, {'Name': 'Quantum Design Liquifier', 'Value': QD}]

                record1 = { 'Time': CURRENT_TIME, 'Dimensions': dimension1, 'MeasureName': 'Volume', 'MeasureValue': str(Volume_data),'MeasureValueType': 'DOUBLE'}
                record2 = { 'Time': CURRENT_TIME, 'Dimensions': dimension2, 'MeasureName': 'Rate', 'MeasureValue': str(Rate_of_Change),'MeasureValueType': 'DOUBLE'}
                record3 = { 'Time': CURRENT_TIME, 'Dimensions': dimension3, 'MeasureName': 'Pactual', 'MeasureValue': str(P_dewar),'MeasureValueType': 'DOUBLE'}
                record4 = { 'Time': CURRENT_TIME, 'Dimensions': dimension4, 'MeasureName': 'Psetpoint', 'MeasureValue': str(SP_press),'MeasureValueType': 'DOUBLE'}
                record4 = { 'Time': CURRENT_TIME, 'Dimensions': dimension5, 'MeasureName': 'Connection', 'MeasureValue': str(Connection_state),'MeasureValueType': 'DOUBLE'}

                records = [record1,record2,record3,record4]
                response = client.write_records(DatabaseName=DB_NAME,TableName=QD,Records=records)
                print(response)
            
        time.sleep(60)


if __name__ == "__main__":
    main()