import datetime as dt

#Class for marking processing time
class ProcessTime:
    """A class used for timing processes - not designed to run over midnight
    -----
    Attributes:
        lap_li -> A list containing timestamps
        st_time -> A single timestamp establish on init
        en_time -> A single timestampe created with end()
    Methods:
        calc_el_time -> Calculates the time between two timestamps
            args:
                st_time - timestamp
                en_time - timestamp
            returns:
                list - Time elapsed in the format [hours,minutes,seconds]
        lap -> Appends a newtimestamp to the class lap_li
        end -> Creates the en_time attribute
        show_lap_times -> calculates and prints all the lap times in lap_li
        show_lap_times -> Find the last lap in lap_li and prints it
            args:
                show_time - bool - False
            returns:
                None
    """
    def __init__(self,name:str = ''):
        self.st_time = dt.datetime.now()
        self.lap_li = []
        self.en_time = None
        self.name = name
    def calc_el_time(self,st_time,en_time):
        diff_time = en_time - st_time
        duration_in_s = diff_time.total_seconds()
        hours = int(divmod(duration_in_s, 3600)[0])
        duration_in_s += -(hours * 3600)
        minutes = int(divmod(duration_in_s, 60)[0])
        duration_in_s += -(minutes * 60)
        seconds = int(duration_in_s)
        return [hours,minutes,seconds]
    def lap(self):
        self.lap_li.append(dt.datetime.now())
    def end(self):
        self.en_time = dt.datetime.now()
        lap_time = self.calc_el_time(self.st_time,self.en_time)
        if self.name != '':
            msg = 'TOTAL ELAPSED TIME OF {} -> {}:{}:{}'.format(self.name,lap_time[0],lap_time[1],lap_time[2])
        else:
            msg = 'TOTAL ELAPSED TIME -> {}:{}:{}'.format(lap_time[0],lap_time[1],lap_time[2])
        return [msg]
    def show_lap_times(self):
        tmp_count = 0
        for lap in self.lap_li:
            tmp_count += 1
            lap_time = self.calc_el_time(self.st_time,lap)
            msg = 'LAP {} TIME -> {}:{}:{}'.format(tmp_count,lap_time[0],lap_time[1],lap_time[2])
        return [msg]
    def show_latest_lap_time(self,show_time:bool=False):
        if len(self.lap_li) == 0:
            return
        elif len(self.lap_li) < 2:
            times = (self.st_time,self.lap_li[-1])
        else:
            times = (self.lap_li[-2],self.lap_li[-1])
        lap_time = self.calc_el_time(times[0],times[1])
        msg = []
        if show_time == True:
            msg.append('LAP {} TIMES -> \n\tSTART: {}\n\t  END: {}'.format(len(self.lap_li),times[0].strftime('%Y-%m-%d %H:%M:%S'),times[1].strftime('%Y-%m-%d %H:%M:%S')))
        msg.append('LAP {} TIME -> {}:{}:{}'.format(len(self.lap_li),lap_time[0],lap_time[1],lap_time[2]))
        times = None
        return msg