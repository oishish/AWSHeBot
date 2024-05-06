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

def retrieve_data_from_Cryomech():
    # Find the last modified file in the data directory
    folder_path = r'//LH2MONITORPC/LHe Log/'
    file_type = r'\*.txt'
    
    files = glob.glob(folder_path + file_type)
    iden = max(files, key=os.path.getctime)

    # Read the data
    data = np.genfromtxt(iden, delimiter='\t', dtype='str')
    rows, cols = data.shape

    t = []
    P = np.zeros(rows)
    Pure = np.zeros(rows)
    CryoP = np.zeros(rows)

    CryoVolume = np.zeros(rows)
    CryoTemp = np.zeros(rows)

    for i in range(rows):
        stime = data[i, 0] + " " + data[i, 1]
        current = datetime.strptime(stime, "%m/%d/%Y %I:%M:%S %p")
        t.append(current)
        P[i] = float(data[i, 17])
        Pure[i] = float(data[i, 18])
        CryoP[i] = float(data[i, 2])
        CryoTemp[i] = float(data[i, 4])
        CryoVolume[i] = (2.97*float(data[i, 5])) - 14.0
    
    return P, Pure, CryoP, CryoTemp, CryoVolume


    
def main():
    ## Initially, retrieve data the Cryomech

    print('Retrieving data from Cryomech')
    MPT_Pressure, Purity_Sensor, Liquifier_Pressure, Liquifier_Temp, Liquififer_Volume = retrieve_data_from_Cryomech()

    print('MPT, Purity sensor, Pressure, Temperature, Volume')
    print(str(MPT_Pressure[-1]), str(Purity_Sensor[-1]), str(Liquifier_Pressure[-1]), str(Liquifier_Temp[-1]), str(Liquififer_Volume[-1]))
    
    CURRENT_TIME = str(int(time.time() * 1000))
    print(CURRENT_TIME)

    client = boto3.client('timestream-write', region_name='us-east-2', aws_access_key_id=ACCESS_ID, aws_secret_access_key= ACCESS_KEY)

    dimension1 = [ {'Name': 'Parameter', 'Value': 'Pressure'}, {'Name': 'Units', 'Value': 'PSI'}, {'Name': 'Medium Pressure Tanks', 'Value': 'Pressure'}]
    dimension2 = [ {'Name': 'Parameter', 'Value': 'Current'}, {'Name': 'Units', 'Value': 'nA'}, {'Name': 'Old purifier', 'Value': 'Purity sensor'}]
    dimension3 = [ {'Name': 'Parameter', 'Value': 'Pressure'}, {'Name': 'Units', 'Value': 'PSI'}, {'Name': 'Cryomech pressure', 'Value': 'Cryomech liquifier'}]
    dimension4 = [ {'Name': 'Parameter', 'Value': 'Temperature'}, {'Name': 'Units', 'Value': 'Kelvin'}, {'Name': 'Cryomech temperature', 'Value': 'Cryomech liquifier'}]
    dimension5 = [ {'Name': 'Parameter', 'Value': 'Volume'}, {'Name': 'Units', 'Value': 'Liters'}, {'Name': 'Cryomech volume', 'Value': 'Cryomech liquifier'}]


    record1 = { 'Time': CURRENT_TIME, 'Dimensions': dimension1, 'MeasureName': 'Pressure', 'MeasureValue': str(MPT_Pressure[-1]),'MeasureValueType': 'DOUBLE'}
    record2 = { 'Time': CURRENT_TIME, 'Dimensions': dimension2, 'MeasureName': 'Current', 'MeasureValue': str(Purity_Sensor[-1]),'MeasureValueType': 'DOUBLE'}
    record3 = { 'Time': CURRENT_TIME, 'Dimensions': dimension3, 'MeasureName': 'Pressure', 'MeasureValue': str(Liquifier_Pressure[-1]),'MeasureValueType': 'DOUBLE'}
    record4 = { 'Time': CURRENT_TIME, 'Dimensions': dimension4, 'MeasureName': 'Temperature', 'MeasureValue': str(Liquifier_Temp[-1]),'MeasureValueType': 'DOUBLE'}
    record5 = { 'Time': CURRENT_TIME, 'Dimensions': dimension5, 'MeasureName': 'Volume', 'MeasureValue': str(Liquififer_Volume[-1]),'MeasureValueType': 'DOUBLE'}

    records = [record1,record2,record3,record4, record5]

    response = client.write_records(DatabaseName=DB_NAME,TableName='Cryomech',Records=records)

    print(response)



    t = datetime.now()
    while True:
        currentTime = datetime.now()
        timedelta = currentTime - t
        print(timedelta.total_seconds())
        if timedelta.total_seconds() >= 60:
            t = currentTime
            print('Retrieving data from Cryomech')
            MPT_Pressure, Purity_Sensor, Liquifier_Pressure, Liquifier_Temp, Liquififer_Volume = retrieve_data_from_Cryomech()

            print('MPT, Purity sensor, Pressure, Temperature, Volume')
            print(str(MPT_Pressure[-1]), str(Purity_Sensor[-1]), str(Liquifier_Pressure[-1]), str(Liquifier_Temp[-1]), str(Liquififer_Volume[-1]))
            
            CURRENT_TIME = str(int(time.time() * 1000))
            print(CURRENT_TIME)

            client = boto3.client('timestream-write', region_name='us-east-2', aws_access_key_id=ACCESS_ID, aws_secret_access_key= ACCESS_KEY)

            dimension1 = [ {'Name': 'Parameter', 'Value': 'Pressure'}, {'Name': 'Units', 'Value': 'PSI'}, {'Name': 'Medium Pressure Tanks', 'Value': 'Pressure'}]
            dimension2 = [ {'Name': 'Parameter', 'Value': 'Current'}, {'Name': 'Units', 'Value': 'nA'}, {'Name': 'Old purifier', 'Value': 'Purity sensor'}]
            dimension3 = [ {'Name': 'Parameter', 'Value': 'Pressure'}, {'Name': 'Units', 'Value': 'PSI'}, {'Name': 'Cryomech pressure', 'Value': 'Cryomech liquifier'}]
            dimension4 = [ {'Name': 'Parameter', 'Value': 'Temperature'}, {'Name': 'Units', 'Value': 'Kelvin'}, {'Name': 'Cryomech temperature', 'Value': 'Cryomech liquifier'}]
            dimension5 = [ {'Name': 'Parameter', 'Value': 'Volume'}, {'Name': 'Units', 'Value': 'Liters'}, {'Name': 'Cryomech volume', 'Value': 'Cryomech liquifier'}]


            record1 = { 'Time': CURRENT_TIME, 'Dimensions': dimension1, 'MeasureName': 'MPT_Pressure', 'MeasureValue': str(MPT_Pressure[-1]),'MeasureValueType': 'DOUBLE'}
            record2 = { 'Time': CURRENT_TIME, 'Dimensions': dimension2, 'MeasureName': 'Current', 'MeasureValue': str(Purity_Sensor[-1]),'MeasureValueType': 'DOUBLE'}
            record3 = { 'Time': CURRENT_TIME, 'Dimensions': dimension3, 'MeasureName': 'Pressure', 'MeasureValue': str(Liquifier_Pressure[-1]),'MeasureValueType': 'DOUBLE'}
            record4 = { 'Time': CURRENT_TIME, 'Dimensions': dimension4, 'MeasureName': 'Temperature', 'MeasureValue': str(Liquifier_Temp[-1]),'MeasureValueType': 'DOUBLE'}
            record5 = { 'Time': CURRENT_TIME, 'Dimensions': dimension5, 'MeasureName': 'Volume', 'MeasureValue': str(Liquififer_Volume[-1]),'MeasureValueType': 'DOUBLE'}

            records = [record1,record2,record3,record4, record5]

            response = client.write_records(DatabaseName=DB_NAME,TableName='Cryomech',Records=records)

            print(response)
            
        time.sleep(60)


if __name__ == "__main__":
    main()