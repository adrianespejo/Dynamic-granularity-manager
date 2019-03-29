from hecuba import *


class IPlogs(StorageDict):
    '''
    @TypeSpec dict<<FK_NUMPERSO:int, PK_ANYOMESDIA:str>, IP_TERMINAL:str, FK_COD_OPERACION:str>
    '''
    pass


class IPConnections(StorageDict):
    '''
    @TypeSpec dict<<IP:str>, connex:set<int>>
    '''
    pass


class DateUsers(StorageDict):
    '''
    @TypeSpec dict<<date:str, IP:str>, users:set<int>>
    '''


class IPUsersInDate(StorageDict):
    '''
    @TypeSpec dict<<IP:str>, users_in_date:set<int, str>>
    '''


class Relationships(StorageDict):
    '''
    @TypeSpec dict<<user:int>, relationships:set<int>>
    '''

