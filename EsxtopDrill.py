import logging
import os
import time

import numpy as np
import pandas as pd

from filters_ops import filer_counter_group

logfile = "./data/logs/esxtopdrill.log"
logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)-15s %(message)s')


class EsxtopDrill():
    def __init__(self, csvfile=None, id=None, dropSysObjects=False):
        if not csvfile:
            logging.error("ERROR: CSV file not provided")
            exit()
        if not id:
            self.id = int(time.time())
        else:
            self.id = id
        self.working_dir = "./data/" + str(self.id) + "/"
        if not (os.path.exists(self.working_dir)):
            os.makedirs(self.working_dir)
        self.csvfile = self.working_dir + csvfile
        self.inputDF = self.load_csv()
        if dropSysObjects:
            self.inputDF = self.drop_sys_obj()
        self.counterGroups = self.get_counterGroupsList()
        self.prep_working_dir()
        self.counters = {}

    def load_csv(self) -> pd.DataFrame:
        try:
            logging.info("Loading " + self.csvfile + " using Default encoding. This may take a while")
            tdf = pd.DataFrame(pd.read_csv(self.csvfile))
            logging.info("CSV file successfully loaded to Memory")
            return tdf
        except Exception as e:
            errmsg = "file read error:" + str(e)
            if "No such file or directory" in errmsg:
                logging.error(errmsg)
                exit()
            if "Unable to allocate" in errmsg:
                logging.error("File too big to load")
                exit()
        try:
            logging.info("Loading " + self.csvfile + " using ISO-8859–1 encoding. This may take a while")
            tdf = pd.DataFrame(pd.read_csv(self.csvfile, encoding='ISO-8859–1'))
            logging.info("CSV file successfully loaded to Memory")
            return tdf
        except Exception as e:
            errmsg = "File Read Error:" + str(e)
            logging.error(errmsg)
            if self.fix_file():
                self.load_csv()

    def fix_file(self):
        logging.info("Attempting to fix file structure")
        numberoflines = 0
        try:
            file = open(self.csvfile, "r")
            for line in file:
                if line:
                    numberoflines = numberoflines + 1
            file.close()
            file = open(self.csvfile, "r")
            fixedfile = []
            i = 0
            numberoflines = numberoflines - 1
            for line in file:
                if i < numberoflines:
                    fixedfile.append(line)
                    i = i + 1
            file.close()
            file = open(self.csvfile, 'w')
            file.writelines(fixedfile)
            file.close()
            logging.info("Fix success, please try to reload the file!")
            return True
        except Exception as e:
            logging.error("Fix failure, please send this file to akalia@vmware.com for debugging" + str(e))
            return False

    def get_counterGroupsList(self):
        col_name_df = pd.DataFrame(self.inputDF.columns)
        c_gn_c_df = col_name_df[0].str.split(("\\"), expand=True)
        cg_df = c_gn_c_df[3].str.split(("\("), expand=True)
        c_gn_c_df[3] = cg_df[0]
        counter_groups = pd.DataFrame(cg_df[0].unique())
        counter_groups.columns = ['Counter Groups']
        counter_groups = counter_groups.replace(to_replace='None', value=np.nan).dropna()
        cg_list = counter_groups['Counter Groups'].tolist()
        return cg_list

    def prep_working_dir(self):
        for i in self.counterGroups:
            path = self.working_dir + str(i)
            if not (os.path.exists(path)):
                os.makedirs(path)

    def get_counterList(self, counterGroup=None):
        if not counterGroup:
            logging.error("Counter Group is required to prepare counter list")
        cg_filtered_data_frame = filer_counter_group(data_frame=self.inputDF, counter_group=counterGroup,
                                                     working_dir=self.working_dir, persist_files=False)
        col_name_df = pd.DataFrame(cg_filtered_data_frame.columns)
        col_name_df = col_name_df.drop([0])
        col_name_df = col_name_df[0].str.split(("\\"), expand=True)
        counterList = list(col_name_df[4].unique())
        return counterList

    def drop_sys_obj(self):
        sys_obj = [
            ':system', ':helper', ':drivers', ':ft', ':vmotion', ':init', ':vmsyslogd', ':sh', ':vobd',
            ':vmkeventd',
            ':vmkdevmgr', ':net-lacp', ':dhclient-uw', ':vmkiscsid', ':nfsgssd', ':busybox', ':ntpd',
            ':vmware-usbarbitrator', ':ioFilterVPServer', ':swapobjd', ':storageRM', ':hostdCgiServer', ':sensord',
            ':net-lbt', ':hostd', ':rhttpproxy', ':slpd', ':net-cdp', ':nscd', ':smartd', ':lwsmd', ':pktcap-agent',
            ':netcpa', ':vdpi', ':logchannellogger', ':logger', ':dcui', ':vpxa', ':fdm', ':vsfwd', ':sfcbd',
            ':sfcb-sfcb',
            ':sfcb-ProviderMa', ':openwsmand', ':sshd', ':esxtop', ':gzip', ':sdrsInjector', ':timeout']
        c_df = pd.DataFrame(self.inputDF.columns)
        c_index_list = list()
        for obj in sys_obj:
            c_index_list.extend(c_df.index[c_df[0].str.contains(obj)].tolist())
        c_name_list = list()
        for c_index in c_index_list:
            c_name = c_df.at[c_index, 0]
            c_name_list.append(c_name)
        out_df = self.inputDF.drop(c_name_list, axis=1)
        logging.info('System objects dropped')
        return out_df


def main():
    test = EsxtopDrill(csvfile="test.csv", id=1, dropSysObjects=False)
    print(test.counterGroups)
    print(test.get_counterList(counterGroup="Memory"))


if __name__ == '__main__':
    main()
