import boto3
import time

DB_NAME = "14T-Logging"
TBL_NAME = "magnet-info"
ACCESS_ID = "AKIASS4P3NHAP7FRYIXV"
ACCESS_KEY = "J3FlheFBEDOI4zS0eFRSwQP/nj/9LHkw9tSw6V4U"

CURRENT_TIME = str(int(time.time() * 1000))
print(CURRENT_TIME)

client = boto3.client('timestream-write', region_name='us-east-2', aws_access_key_id=ACCESS_ID, aws_secret_access_key= ACCESS_KEY)

dimension1 = [ {'Name': 'Parameter', 'Value': 'Field Magnitude'}, {'Name': 'Units', 'Value': 'Tesla'}, {'Name': 'cryogenic-system', 'Value': '14T'}]
dimension2 = [ {'Name': 'Parameter', 'Value': 'Voltage Source'}, {'Name': 'Units', 'Value': 'Volts'}, {'Name': 'cryogenic-system', 'Value': '14T'}]
dimension3 = [ {'Name': 'Parameter', 'Value': 'Voltage Measured'}, {'Name': 'Units', 'Value': 'Volts'}, {'Name': 'cryogenic-system', 'Value': '14T'}]
dimension4 = [ {'Name': 'Parameter', 'Value': 'Switch Heater Voltage'}, {'Name': 'Units', 'Value': 'Volts'}, {'Name': 'cryogenic-system', 'Value': '14T'}]

record1 = { 'Time': CURRENT_TIME, 'Dimensions': dimension1, 'MeasureName': 'Bfield', 'MeasureValue': '9.0','MeasureValueType': 'DOUBLE'}
record2 = { 'Time': CURRENT_TIME, 'Dimensions': dimension2, 'MeasureName': 'Vsource', 'MeasureValue': '0.6','MeasureValueType': 'DOUBLE'}
record3 = { 'Time': CURRENT_TIME, 'Dimensions': dimension3, 'MeasureName': 'Vmeas', 'MeasureValue': '0.001','MeasureValueType': 'DOUBLE'}
record4 = { 'Time': CURRENT_TIME, 'Dimensions': dimension4, 'MeasureName': 'Vswitch', 'MeasureValue': '5.12','MeasureValueType': 'DOUBLE'}

records = [record1,record2,record3,record4]

response = client.write_records(DatabaseName=DB_NAME,TableName=TBL_NAME,Records=records)

print(response)