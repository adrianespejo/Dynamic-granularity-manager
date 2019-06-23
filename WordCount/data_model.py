from hecuba import StorageDict


class Words(StorageDict):
    '''
    @TypeSpec dict<<position:int>, words:str>
    '''


class Result(StorageDict):
    '''
    @TypeSpec dict<<word:str>, instances:int>
    '''
