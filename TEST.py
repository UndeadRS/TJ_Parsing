import os
import re
import pyodbc
from datetime import timedelta, datetime

# Путь технологического журнала
TJ_Path = r'C:\Users\user\Desktop\TJ_82'

# Поиск файлов ТЖ
tj_paths = []
path_generator = os.walk(TJ_Path)
for pars_str in path_generator:
    tj_paths.append(pars_str)

# Подключение к СУБД
Server = 'DESKTOP-FB0HL91'
Database = 'Undead'
connection_string = 'Driver={ODBC Driver 17 for SQL Server};Server=' + Server + \
                    ';Database=' + Database + ';Trusted_Connection=yes'
connection = pyodbc.connect(connection_string, autocommit=True)
dbCursor = connection.cursor()

# Предыдущий час в формате даты и строки: 22040515
date = datetime.today() - timedelta(hours=1)
# date_s = datetime.strftime(date, '%y%m%d%H')
date_s = '22031711'
# Директории файлов событий
TJ_CALL = []
TJ_ADMIN = []
TJ_DESIGNER = []
TJ_ERR = []
TJ_MEM = []
TJ_MSSQL = []
TJ_NOTLOCK = []
TJ_SCALL = []
TJ_SDBL = []

for address, dirs, files in tj_paths:
    for file in files:
        if date_s in file:
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

# Метрики
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
lock_type = []
WaitConnections = []
Context = []
Func = []
Module = []
Method = []
Interface = []

# Очистка массивов
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
    lock_type.clear()
    WaitConnections.clear()
    Context.clear()
    deadlock_session_1.clear()
    deadlock_session_2.clear()
    deadlock_region.clear()
    deadlock_type.clear()
    deadlock_locks.clear()

# Парсинг метрик

# Формирование даты
def date_forming(time_duration):
    pre_min_sec_mikrosec = ''.join(re.findall("(\d+:\d+.\d*?)-", time_duration))
    pre_year_mouth_day_hour = datetime.strftime(date, '%Y-%m-%d %H:')
    filedate = pre_year_mouth_day_hour + pre_min_sec_mikrosec
    event_datetime.append(filedate)
# Формирование длительности
def duration_forming(time_duration):
    duration.append(re.split('-', time_duration)[1])
# Формирование события
def event_forming(split_str):
    event.append(split_str[1])
# Формирование уровня события
def eventlevel_forming(split_str):
    event_level.append(split_str[2])
# Формирование процесса
def process_forming(pars_str):
    process_finder = ''.join(re.findall('process=(.*?),', pars_str))
    process.append(process_finder)
# Формирование имени процесса
def processName_forming(pars_str):
    processName_finder = ''.join(re.findall('processName=(.*?),', pars_str))
    processName.append(processName_finder)
# Формирование номера TCP соединения
def clientID_forming(pars_str):
    clientID_finder = ''.join(re.findall('clientID=(.*?),', pars_str))
    clientID.append(clientID_finder)
# Формирование имени приложения
def applicationName_forming(pars_str):
    applicationName_finder = ''.join(re.findall('applicationName=(.*?),', pars_str))
    applicationName.append(applicationName_finder)
# Формирование имени компьютера
def computerName_forming(pars_str):
    computerName_finder = ''.join(re.findall('computerName=(.*?),', pars_str))
    computerName.append(computerName_finder)
# Формирование номера соединения с ИБ
def connectID_forming(pars_str):
    connectID_finder = ''.join(re.findall('connectID=(.*?),', pars_str))
    connectID.append(connectID_finder)
# Формирование номера сеанса
def SessionID_forming(pars_str):
    SessionID_finder = ''.join(re.findall('SessionID=(.*?),', pars_str))
    SessionID.append(SessionID_finder)
# Формирование имени пользователя
def Usr_forming(pars_str):
    Usr_finder = ''.join(re.findall('Usr=(.*?),', pars_str))
    Usr.append(Usr_finder)
# Формирование таблицы блокировки
def Regions_forming(pars_str):
    Regions_finder = ''.join(re.findall('Regions=(.*?),', pars_str))
    Regions.append(Regions_finder)
# Формирование полей блокировок
def Locks_forming(pars_str):
    Locks_finder = ''.join(re.findall('Locks=(.*?),', pars_str)).strip("'")
    if re.search('TLOCK',pars_str) != None:
        lock_type.append(re.split(' ', Locks_finder)[1])
        Locks.append(re.sub(r'(\w+)[.](\w+)[ ](\w+)','',Locks_finder))
    else:
        lock_type.append('')
        Locks.append('')
# Формирование номера блокируемого сеанса
def WaitConnections_forming(pars_str):
    WaitConnections_finder = ''.join(re.findall('WaitConnections=\'(.*?)\'', pars_str))
    if WaitConnections_finder == '':
        WaitConnections_finder = ''.join(re.findall('WaitConnections=(.*?),', pars_str))
    elif ''.join(re.findall('WaitConnections=(.*?),', pars_str)) =='':
        WaitConnections_finder = ''.join(re.findall('WaitConnections=(.*)', pars_str))
    WaitConnections.append(WaitConnections_finder)
# Формирование контекста
def Context_forming(pars_str):
    Context_finder = ''.join(re.findall('Context=\'(.*?)\'', pars_str))
    Context.append(Context_finder.strip("'"))
# Формирование описания Дедлока
def DeadlockConnectionIntersections_forming(pars_str):
    DeadlockConnectionIntersections_finder = ''.join(re.findall('DeadlockConnectionIntersections=(.*?),', pars_str))
    if re.search('TDEADLOCK',pars_str) == None:
        deadlock_session_1.append('')
        deadlock_session_2.append('')
        deadlock_region.append('')
        deadlock_type.append('')
        deadlock_locks.append('')
    else:
        deadlock_session_1.append((re.split(' ',DeadlockConnectionIntersections_finder)[0]).strip("'"))
        deadlock_session_2.append((re.split(' ',DeadlockConnectionIntersections_finder)[1]).strip("'"))
        deadlock_region.append(re.split(' ',DeadlockConnectionIntersections_finder)[2])
        deadlock_type.append(re.split(' ',DeadlockConnectionIntersections_finder)[3])
        deadlock_locks.append(re.split(' ',DeadlockConnectionIntersections_finder,maxsplit=4)[4])

# Открытие файла и форматирование для парсинга
def open_file(one_path, ready_file):
    open_file = open(one_path, encoding='utf-8')
    file_read = open_file.read()
    one_string = re.sub('\t*|\n*|\r*', '', file_read)
    split_one_string = re.split("(\d{2}:\d+.\d*?-)", one_string)
    del split_one_string[0]
    len_string = len(split_one_string) - 1

    n = 0

    while n <= len_string:
        if n == len_string:
            ready_file.append(split_one_string[n])
        else:
            ready_file.append(split_one_string[n] + split_one_string[n + 1])
        n += 2

    open_file.close()

# Парсинг NOTLOCK
def TJ_NOTLOCK_Pars():
    for one_path in TJ_NOTLOCK:
        ready_file = []
        open_file(one_path, ready_file)

        for pars_str in ready_file:
            split_str = re.split(',', pars_str)
            time_duration = split_str[0]
            date_forming(time_duration)
            duration_forming(time_duration)
            event_forming(split_str)
            eventlevel_forming(split_str)
            process_forming(pars_str)
            processName_forming(pars_str)
            clientID_forming(pars_str)
            applicationName_forming(pars_str)
            computerName_forming(pars_str)
            connectID_forming(pars_str)
            SessionID_forming(pars_str)
            Usr_forming(pars_str)
            DeadlockConnectionIntersections_forming(pars_str)
            Regions_forming(pars_str)
            Locks_forming(pars_str)
            WaitConnections_forming(pars_str)
            Context_forming(pars_str)

    # Выгрузка в СУБД
    len_string = len(event_datetime) - 1
    n = 0

    while n <= len_string:
        if event[n] == 'TLOCK':
            dbCursor.execute(f"INSERT INTO TLOCK(event_datetime,duration,event,event_level,process,processName,clientID,\
            applicationName,computerName,connectID,SessionID,Usr,Regions,lock_type,Locks,\
            WaitConnections, Context)\
            VALUES (N'{event_datetime[n]}',N'{duration[n]}',N'{event[n]}',N'{event_level[n]}',N'{process[n]}',\
            N'{processName[n]}',N'{clientID[n]}',N'{applicationName[n]}',N'{computerName[n]}',N'{connectID[n]}',\
            N'{SessionID[n]}',N'{Usr[n]}',N'{Regions[n]}',N'{lock_type[n]}',N'{Locks[n]}',\
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

    pyodbc.pooling = False

    # Очистка массивов после выгрузки
    clear_array()

TJ_NOTLOCK_Pars()