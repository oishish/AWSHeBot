import MySQLdb as mysql
from datetime import datetime



def extrapolate(extrapolated_times, ema, data, times):
    print("extrapolating...")
    extrapolated_data = list()
    for i in range(len(extrapolated_times)):
        if data[-1] + ema*((extrapolated_times[i] - times[-1]).total_seconds()) <0:
            extrapolated_data.append(0)
        else:
            extrapolated_data.append(data[-1] + ema*((extrapolated_times[i] - times[-1]).total_seconds()))
    return extrapolated_data

def ema(times, data, alpha):
    ema = 0
    for i in range(len(data) - 1):
        if data[i+1] - data[i]<0 and (data[i+1] - data[i])/((times[i+1] - times[i]).total_seconds())>-0.001:
            ema = alpha*((data[i+1] - data[i])/((times[i+1] - times[i]).total_seconds()))+(1-alpha)*ema
    return ema

def interpolate_fast(interpolated_times, data, times):
    interpolated_data = list()
    i = 0
    j = 0
    while i < len(interpolated_times):
        dt = (times[j + 1] - times[j]).total_seconds()
        dV = data[j + 1] - data[j]
        k = dV / dt
        if (interpolated_times[i] - times[j]).total_seconds() >=0 and (interpolated_times[i] - times[j + 1]).total_seconds() <=0:
            interpolated_data.append(data[j] + k * (interpolated_times[i] - times[j]).total_seconds())
            i += 1
        else:
            if interpolated_times[i]<times[0]:
                interpolated_data.append(0)
            else:
                if interpolated_times[i] < times[j]:
                    j -= 1
                    print(times[j])
                else:
                    if interpolated_times[i] > times[j+1]:
                        j += 1
    return interpolated_data


def delete_last_n_rows():
    print('connecting to MySql')
    conn = mysql.connect('gator4099.hostgator.com','afy2003_LHeBufferBot','rwnVv3%MXns3j;X{','afy2003_LHeMonitor')
    print("connection has been established")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM NetSystemVolume ORDER BY 'Time' DESC LIMIT 10000000;")
    conn.commit()
    conn.close()
    print("done deleting")

def uploadAllData(Onep5k_times1, Net_system_Volume, He3_volume1, Onep5k_volume1):
    print('connecting to MySql')
    conn = mysql.connect('gator4099.hostgator.com','afy2003_LHeBufferBot','rwnVv3%MXns3j;X{','afy2003_LHeMonitor')
    print("connection has been established")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM  `NetSystemVolume` ORDER BY 'Time' DESC LIMIT 5000000;")
    Netsystem = cursor.fetchall()
    NumberOfOldPoints = len([list(i) for i in Netsystem])
    TimeLastUpdated = [datetime.strptime(i[0], '%Y-%m-%d %H:%M:%S') for i in Netsystem]
    if NumberOfOldPoints>0:
        t = 0
        for i in range(len(Onep5k_times1)):
            if (Onep5k_times1[i]-TimeLastUpdated[-1]).total_seconds()>0:
                cursor.execute("INSERT INTO NetSystemVolume VALUES (%s,%s,%s,%s)", (str(Onep5k_times1[i]), str(Onep5k_volume1[i]), str(He3_volume1[i]), str(Net_system_Volume[i])))
                t+=1
        print(t)
        conn.commit()
        conn.close()
        return 0
    else:
        for i in range(len(Net_system_Volume)):
            cursor.execute("INSERT INTO NetSystemVolume VALUES (%s,%s,%s,%s)", (str(Onep5k_times1[i]), str(Onep5k_volume1[i]), str(He3_volume1[i]), str(Net_system_Volume[i])))
        print('done uploading')
        conn.commit()
        conn.close()
        return 0


# extracting data
conn = mysql.connect('gator4099.hostgator.com','afy2003_LHeBufferBot','rwnVv3%MXns3j;X{','afy2003_LHeMonitor')
print("connection has been established")
cursor = conn.cursor()
cursor.execute("SELECT * FROM  `15K` ORDER BY `15K_Time_Last_Updated` ASC LIMIT 5000000;")
values1p5K = cursor.fetchall()
cursor.execute("SELECT * FROM `300mK` ORDER BY `300mk_Time_Last_Updated` ASC LIMIT 5000000;")
valuesHe3 = cursor.fetchall()
cursor.execute("SELECT * FROM `Status` ORDER BY `CurrentTime` ASC LIMIT 5000000;")
valuesLiquifiers = cursor.fetchall()

valuesHe3 = [list(i) for i in valuesHe3]
values1p5K = [list(i) for i in values1p5K]
valuesLiquifiers = [list(i) for i in valuesLiquifiers]

He3_percent = [i[0] for i in valuesHe3]
He3_level = [i[1] for i in valuesHe3]
He3_volume = [i[2] for i in valuesHe3]
He3_times = [datetime.strptime(time[3], '%Y%m%d%H%M%S') for time in valuesHe3]


Onep5k_percent = [i[0] for i in values1p5K]
Onep5k_level = [i[1] for i in values1p5K]
Onep5k_volume = [i[2] for i in values1p5K]
Onep5k_times = [datetime.strptime(time[3], '%Y%m%d%H%M%S') for time in values1p5K]


Cryomech_Volume = [float(i[6]) for i in valuesLiquifiers]
QD1_Volume = [float(i[25]) for i in valuesLiquifiers]
QD2_Volume = [float(i[21]) for i in valuesLiquifiers]
QD3_Volume = [float(i[31]) for i in valuesLiquifiers]
Buffer = [204.0/400.0*float(i[17]) for i in valuesLiquifiers]

MasterTimes = [datetime.strptime(time[20], '%Y%m%d%H%M%S') for time in valuesLiquifiers]


# interpolating and extrapolating 300mk data
MasterExtrapolate = list()
MasterInterpolate = list()
for i in MasterTimes:
    if (i - He3_times[-1]).total_seconds()<0:
        MasterInterpolate.append(i)
    else:
        MasterExtrapolate.append(i)


Interpolated_Volume = interpolate_fast(MasterInterpolate, He3_volume, He3_times)
ema3He = ema(He3_times, He3_volume, 0.8)
Extrapolated_Volume = extrapolate(MasterExtrapolate, ema3He, He3_volume, He3_times)
He3_volume_final = Interpolated_Volume + Extrapolated_Volume
He3_times_final = MasterInterpolate + MasterExtrapolate


# interpolating and extrapolating the 1p5K Data

MasterExtrapolate = list()
MasterInterpolate = list()
for i in MasterTimes:
    if i < Onep5k_times[-1]:
        MasterInterpolate.append(i)
    else:
        MasterExtrapolate.append(i)


Interpolated_Volume = interpolate_fast(MasterInterpolate, Onep5k_volume, Onep5k_times)
ema1p5K = ema(Onep5k_times, Onep5k_volume, 0.005)
Extrapolated_Volume = extrapolate(MasterExtrapolate, ema1p5K, Onep5k_volume, Onep5k_times)
Onep5k_volume_final = Interpolated_Volume+Extrapolated_Volume
Onep5k_times_final = MasterInterpolate + MasterExtrapolate


Net_system_Volume =[Onep5k_volume_final[i]+He3_volume_final[i] + QD1_Volume[i] + QD2_Volume[i] + QD3_Volume[i] + Cryomech_Volume[i] + Buffer[i] for i in range(len(MasterTimes))]
print(len(Onep5k_volume_final), len(MasterTimes))

print(Net_system_Volume[len(Net_system_Volume)-1])
print(MasterTimes[-1])
# plt.plot(MasterTimes, He3_volume_final)
# plt.plot(He3_times, He3_volume, 'ro')
# plt.show()
# plt.plot(MasterTimes, Onep5k_volume_final)
# plt.plot(Onep5k_times, Onep5k_volume, 'ro')
# plt.show()
delete_last_n_rows()
uploadAllData(Onep5k_times_final, Net_system_Volume, He3_volume_final, Onep5k_volume_final)
