"""
1. Получаем список файлов для парсинга
2. Поочередное чтение каждого файла
2. Поочередный парсинг строки
3. Запись метрик
4. Выгрузка записей в БД
"""

import os
import re
import pyodbc
from datetime import timedelta, datetime

'''Подключение к СУБД'''
Server = 'AINURKORS-PC'
Database = 'rarus_tj_analyzer_2'
connectionString = 'Driver={ODBC Driver 17 for SQL Server};Server=' + Server + \
                   ';Database=' + Database + ';Trusted_Connection=yes'
connection = pyodbc.connect(connectionString, autocommit=True)
dbCursor = connection.cursor()

'''Формирование абсолютного пути файлов для парсинга'''
TJ_CALL = []
TJ_ADMIN = []
TJ_DESIGNER = []
TJ_ERR = []
TJ_MEM = []
TJ_MSSQL = []
TJ_NOTLOCK = []
TJ_SCALL = []
TJ_SDBL = []

'''Списки данных метрик'''
event_datetime = []
duration = []
event = []
event_level = []
process = []
processName = []
clientID = []
applicationName = []
computerName = []
connectID = []
SessionID = []
Usr = []
DeadlockConnectionIntersections = []
deadlock_session_1 = []
deadlock_session_2 = []
deadlock_region = []
deadlock_type = []
deadlock_locks = []
Regions = []
Locks = []
WaitConnections = []
Context = []
Func = []
Module = []
Method = []
Interface = []

'''Предыдущий час в формате даты и строки: 22040515'''
last_hour_date = datetime.today() - timedelta(hours=1)
last_hour_date_str = datetime.strftime(last_hour_date, '%y%m%d%H')
last_hour_date_str = '22222221'

'''Поиск файлов ТЖ'''
tj_paths = []
path_generator = os.walk(r"D:\TJ\TJ_FULL")
for pars_str in path_generator:
    tj_paths.append(pars_str)

'''Формирование абсолютного пути файлов для парсинга'''
for address, dirs, files in tj_paths:
    for file in files:
        if last_hour_date_str in file:
            result = os.path.getsize(address + '\\' + file)
            if result > 3:
                if 'TJ_CALL' in address:
                    TJ_CALL.append(address + '\\' + file)
                if 'TJ_ADMIN' in address:
                    TJ_ADMIN.append(address + '\\' + file)
                if 'TJ_DESIGNER' in address:
                    TJ_DESIGNER.append(address + '\\' + file)
                if 'TJ_ERR' in address:
                    TJ_ERR.append(address + '\\' + file)
                if 'TJ_MEM' in address:
                    TJ_MEM.append(address + '\\' + file)
                if 'TJ_MSSQL' in address:
                    TJ_MSSQL.append(address + '\\' + file)
                if 'TJ_NOTLOCK' in address:
                    TJ_NOTLOCK.append(address + '\\' + file)
                if 'TJ_SCALL' in address:
                    TJ_SCALL.append(address + '\\' + file)
                if 'TJ_SDBL' in address:
                    TJ_SDBL.append(address + '\\' + file)

    '''Очистка массивов с данными после выгрузки в СУБД'''
def clear_array():
    event_datetime.clear()
    duration.clear()
    event.clear()
    event_level.clear()
    process.clear()
    processName.clear()
    clientID.clear()
    applicationName.clear()
    computerName.clear()
    connectID.clear()
    SessionID.clear()
    Usr.clear()
    Func.clear()
    Module.clear()
    Method.clear()
    Interface.clear()
    DeadlockConnectionIntersections.clear()
    Regions.clear()
    Locks.clear()
    WaitConnections.clear()
    Context.clear()
    deadlock_session_1.clear()
    deadlock_session_2.clear()
    deadlock_region.clear()
    deadlock_type.clear()
    deadlock_locks.clear()



'''Парсинг метрик'''
def date_forming():
    pre_min_sec_mikrosec = ''.join(re.findall("(\d+:\d+.\d*?)-", time_duration))
    pre_year_mouth_day_hour = datetime.strftime(last_hour_date, '%Y-%m-%d %H:')
    filedate = pre_year_mouth_day_hour + pre_min_sec_mikrosec
    event_datetime.append(filedate)

'''Формирование длительности'''
def duration_forming():
    duration.append(re.split('-', time_duration)[1])

'''Формирование события'''
def event_forming():
    event.append(split_str[1])

'''Формирование уровня события'''

def eventlevel_forming():
    event_level.append(split_str[2])

'''Формирование процесса'''
def process_forming():
    process_finder = ''.join(re.findall('process=(.*?),', pars_str))
    process.append(process_finder)

'''Формирование имени процесса'''
def processName_forming():
    processName_finder = ''.join(re.findall('processName=(.*?),', pars_str))
    processName.append(processName_finder)

'''Формирование номера TCP соединения'''
def clientID_forming():
    clientID_finder = ''.join(re.findall('clientID=(.*?),', pars_str))
    clientID.append(clientID_finder)

'''Формирование имени приложения'''
def applicationName_forming():
    applicationName_finder = ''.join(re.findall('applicationName=(.*?),', pars_str))
    applicationName.append(applicationName_finder)

'''Формирование имени компьютера'''
def computerName_forming():
    computerName_finder = ''.join(re.findall('computerName=(.*?),', pars_str))
    computerName.append(computerName_finder)

'''Формирование номера соединения с ИБ'''
def connectID_forming():
    connectID_finder = ''.join(re.findall('connectID=(.*?),', pars_str))
    connectID.append(connectID_finder)

'''Формирование номера сеанса'''
def SessionID_forming():
    SessionID_finder = ''.join(re.findall('SessionID=(.*?),', pars_str))
    SessionID.append(SessionID_finder)

'''Формирование имени пользователя'''
def Usr_forming():
    Usr_finder = ''.join(re.findall('Usr=(.*?),', pars_str))
    Usr.append(Usr_finder)

'''Формирование таблицы блокировки'''
def Regions_forming():
    Regions_finder = ''.join(re.findall('Regions=(.*?),', pars_str))
    Regions.append(Regions_finder)

'''Формирование полей блокировок'''
def Locks_forming():
    Locks_finder = ''.join(re.findall('Locks=(.*?),', pars_str))
    Locks.append(Locks_finder.strip("'"))

'''Формирование номера блокируемого сеанса'''
def WaitConnections_forming():
    WaitConnections_finder = ''.join(re.findall('WaitConnections=\'(.*?)\'', pars_str))
    if WaitConnections_finder == '':
        WaitConnections_finder = ''.join(re.findall('WaitConnections=(.*?),', pars_str))
    elif ''.join(re.findall('WaitConnections=(.*?),', pars_str)) =='':
        WaitConnections_finder = ''.join(re.findall('WaitConnections=(.*)', pars_str))
    WaitConnections.append(WaitConnections_finder)


'''Формирование контекста'''
def Context_forming():
    Context_finder = ''.join(re.findall('Context=\'(.*?)\'', pars_str))
    Context.append(Context_finder.strip("'"))

'''Формирование описания Дедлока'''
def DeadlockConnectionIntersections_forming():
    DeadlockConnectionIntersections_finder = ''.join(re.findall('DeadlockConnectionIntersections=(.*?),', pars_str))
    if re.search('TDEADLOCK',pars_str) == None:
        deadlock_session_1.append('')
        deadlock_session_2.append('')
        deadlock_region.append('')
        deadlock_type.append('')
        deadlock_locks.append('')
    else:
        # print('DeadlockConnectionIntersections_finder=',DeadlockConnectionIntersections_finder)
        deadlock_session_1.append((re.split(' ',DeadlockConnectionIntersections_finder)[0]).strip("'"))
        deadlock_session_2.append((re.split(' ',DeadlockConnectionIntersections_finder)[1]).strip("'"))
        deadlock_region.append(re.split(' ',DeadlockConnectionIntersections_finder)[2])
        deadlock_type.append(re.split(' ',DeadlockConnectionIntersections_finder)[3])
        deadlock_locks.append(re.split(' ',DeadlockConnectionIntersections_finder,maxsplit=4)[4])
        print('deadlock_session_1=',deadlock_session_1)
        print('deadlock_session_2=',deadlock_session_2)
        print('deadlock_region=',deadlock_region)
        print('deadlock_locks=',deadlock_locks)


    # DeadlockConnectionIntersections.append(DeadlockConnectionIntersections_finder)
    # print('DeadlockConnectionIntersections=',DeadlockConnectionIntersections)



for one_path in TJ_NOTLOCK:
    if 'TJ_NOTLOCK' in one_path:

        file_open = open(one_path, encoding='utf-8')
        file_read = file_open.read()
        one_string = re.sub('\t*|\n*|\r*', '', file_read)
        split_one_string = re.split("(\d{2}:\d+.\d*?-)", one_string)
        del split_one_string[0]
        len_string = len(split_one_string) - 1

        ready_file = []

        n = 0

        while n <= len_string:
            if n == len_string:
                ready_file.append(split_one_string[n])
            else:
                ready_file.append(split_one_string[n] + split_one_string[n + 1])
            n += 2

        for pars_str in ready_file:
            split_str = re.split(',', pars_str)
            time_duration = split_str[0]

            '''Сбор метрик для NOTLOCK'''
            date_forming()
            duration_forming()
            event_forming()
            eventlevel_forming()
            process_forming()
            processName_forming()
            clientID_forming()
            applicationName_forming()
            computerName_forming()
            connectID_forming()
            SessionID_forming()
            Usr_forming()
            DeadlockConnectionIntersections_forming()
            Regions_forming()
            Locks_forming()
            WaitConnections_forming()
            Context_forming()

        file_open.close()

'''Выгрузка в СУБД'''
len_string = len(event_datetime) - 1
n = 0

while n <= len_string:
    # print('event_datetime=',event_datetime[n])
    # print('duration=',duration[n])
    # print('event=',event[n])
    # print('event_level=',event_level[n])
    # print('process=',process[n])
    # print('processName=',processName[n])
    # print('clientID=',clientID[n])
    # print('applicationName=',applicationName[n])
    # print('computerName=',computerName[n])
    # print('connectID=',connectID[n])
    # print('SessionID=',SessionID[n])
    # print('Usr=',Usr[n])
    # print('Regions=',Regions[n])
    # print('Locks=',Locks[n])
    # print('WaitConnections=',WaitConnections[n])
    # print('Context=',Context[n])
    if event[n] == 'TLOCK':
        dbCursor.execute(f"INSERT INTO TLOCK(event_datetime,duration,event,event_level,process,processName,clientID,\
        applicationName,computerName,connectID,SessionID,Usr,Regions,Locks,\
        WaitConnections, Context)\
        VALUES (N'{event_datetime[n]}',N'{duration[n]}',N'{event[n]}',N'{event_level[n]}',N'{process[n]}',\
        N'{processName[n]}',N'{clientID[n]}',N'{applicationName[n]}',N'{computerName[n]}',N'{connectID[n]}',\
        N'{SessionID[n]}',N'{Usr[n]}',N'{Regions[n]}',N'{Locks[n]}',\
        N'{WaitConnections[n]}',N'{Context[n]}')")
    elif event[n] == 'TTIMEOUT':
        dbCursor.execute(f"INSERT INTO TTIMEOUT(event_datetime,event,event_level,process,processName,clientID,\
        applicationName,computerName,connectID,SessionID,Usr,WaitConnections,Context)\
        VALUES (N'{event_datetime[n]}',N'{event[n]}',N'{event_level[n]}',N'{process[n]}',\
        N'{processName[n]}',N'{clientID[n]}',N'{applicationName[n]}',N'{computerName[n]}',N'{connectID[n]}',\
        N'{SessionID[n]}',N'{Usr[n]}',N'{WaitConnections[n]}',N'{Context[n]}')")
    elif event[n] == 'TDEADLOCK':
        dbCursor.execute(f"INSERT INTO TDEADLOCK(event_datetime,event,event_level,process,processName,clientID,\
        applicationName,computerName,connectID,SessionID,Usr,deadlock_session_1,deadlock_session_2,deadlock_region,deadlock_type,deadlock_locks,Context)\
        VALUES (N'{event_datetime[n]}',N'{event[n]}',N'{event_level[n]}',N'{process[n]}',\
        N'{processName[n]}',N'{clientID[n]}',N'{applicationName[n]}',N'{computerName[n]}',N'{connectID[n]}',\
        N'{SessionID[n]}',N'{Usr[n]}',N'{deadlock_session_1[n]}',N'{deadlock_session_2[n]}',N'{deadlock_region[n]}',N'{deadlock_type[n]}',N'{deadlock_locks[n]}',N'{Context[n]}')")

    n += 1

clear_array()
pyodbc.pooling = False