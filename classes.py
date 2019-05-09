from hecuba import StorageDict, StorageObj


class GridPoints(StorageDict):
    '''
    @TypeSpec dict<<lat:double,lon:double,time:int, ilev:int>, m_cloudfract:double, m_humidity:double, m_icewater:double, m_liquidwate:double, m_ozone:double, m_pot_vorticit:double, m_rain:double, m_snow:double>
    '''


class GlobalStats(StorageDict):
    '''
    @TypeSpec dict<<time:int, lat:double, lon:double>, m_cloudfract:classes.Stats, m_humidity:classes.Stats, m_icewater:classes.Stats, m_liquidwate:classes.Stats, m_ozone:classes.Stats, m_pot_vorticit:classes.Stats, m_rain:classes.Stats, m_snow:classes.Stats>
    '''

    num_metrics = 8

    def __missing__(self, key):
        l = [Stats() for _ in range(self.num_metrics)]
        for i in l:
            i.sum = 0.0
            i.min = float("Inf")
            i.max = float("-Inf")
            i.count = 0
        return l


class Stats(StorageObj):
    """
    @ClassField sum double
    @ClassField min double
    @ClassField max double
    @ClassField count int
    """

    def get_avg(self):
        return float(self.sum / self.count)
