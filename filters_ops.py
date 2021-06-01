import logging
import os
import time
from datetime import datetime

import pandas as pd

logfile = "./data/logs/esxtopdrill.log"
logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)-15s %(message)s')


# --------------------------------------------------------------------------
# Function to implement object filtration
def filter_objects(working_dir=None, data_frame=None, object_selection=None, persist_files=True):
    if working_dir is None or data_frame is None or object_selection is None:
        logging.error("Function filter_objects called without working_dir, data_frame or object_selection")
        return

    for i in object_selection:
        try:
            int(i)
            input_error = input_error + 1
        except ValueError:
            logging.error("Interger value not allowed for object_selection")

    cdf = pd.DataFrame(data_frame.columns)
    cdf.columns = ['Objects']
    out_df = data_frame
    if len(object_selection) != 0:
        column_list = []
        for Object in object_selection:
            column_list.extend(cdf.index[cdf['Objects'].str.contains(Object)].tolist())
            column_list.sort(reverse=False)
        col_name = []
        col_name.insert(0, cdf['Objects'][0])
        i = 1
        for col in column_list:
            col_name.insert(i, cdf['Objects'][col])
            i = i + 1
            out_df = data_frame[col_name]
        col_name_df = pd.DataFrame(out_df.columns)
        if col_name_df[0].count() <= 1:
            logging.info("Unable to find the objects specified")
            return data_frame
        if persist_files:
            outfile = str(object_selection[0] + str(int(time.time())) + ".csv")
            outfile = os.path.join(working_dir, outfile)
            out_df.to_csv(outfile, index=False)
            logging.info("Generated: " + outfile)
    return out_df


# --------------------------------------------------------------------------------------
# Function to implement Counter Group filtration
def filer_counter_group(data_frame=None, counter_group=None, working_dir=None, persist_files=True):
    if working_dir is None or data_frame is None or counter_group is None:
        logging.error("Function filer_counter_group called without working_dir, data_frame or counter_group")
        return

    col_name_df = pd.DataFrame(data_frame.columns)
    c_gn_c_df = col_name_df[0].str.split(("\\"), expand=True)
    cg_df = c_gn_c_df[3].str.split(("\("), expand=True)
    c_gn_c_df[3] = cg_df[0]
    column_list = [0]
    column_list.extend(c_gn_c_df.index[c_gn_c_df[3] == counter_group].tolist())
    column_list.sort(reverse=False)
    col_name = []
    i = 0
    for col in column_list:
        col_name.insert(i, col_name_df[0][col])
        i = i + 1
    out_df = data_frame[col_name]
    if persist_files:
        outfile = str(counter_group + "-" + str(int(time.time())) + ".csv")
        outfile = os.path.join(working_dir, counter_group, outfile)
        out_df.to_csv(outfile, index=False)
        logging.info("Generated: " + outfile)
    return out_df


# ------------------------------------------------------------------------------------------
# Function to implement Counter filtration
def filer_counter(data_frame=None, counter=None, counter_group=None, working_dir=None, persist_files=True):
    if working_dir is None or data_frame is None or counter_group is None or counter is None:
        logging.error("Function filer_counter called without working_dir, data_frame,counter_group or counter")
        return
    out_df = data_frame
    col_name_df = pd.DataFrame(data_frame.columns)
    time_se = str(col_name_df[0][0])
    col_name_df = col_name_df.drop([0])
    c_gn_c_df = col_name_df[0].str.split(("\\"), expand=True)
    column_list = list()
    try:
        column_list.extend(c_gn_c_df.index[c_gn_c_df[4] == counter].tolist())
        column_list.sort(reverse=False)
        col_name = list()
        col_name.append(time_se)
        i = 1
        for Col in column_list:
            col_name.insert(i, col_name_df[0][Col])
            i = i + 1
        out_df = out_df[col_name]
        try:
            if persist_files:
                outfile = str(counter_group + "-" + counter + "-" + str(int(time.time())) + ".csv")
                outfile = os.path.join(working_dir, counter_group, outfile)
                out_df.to_csv(outfile, index=False)
                logging.info("Generated: " + outfile)
        except (FileNotFoundError, OSError):
            outfile = str(counter_group + "-" + counter + "-" + str(int(time.time())) + ".csv")
            outfile = outfile.replace("/", "-")
            outfile = outfile.replace("?", " ")
            outfile = os.path.join(working_dir, counter_group, outfile)
            out_df.to_csv(outfile, index=False)
            logging.info("Generated: " + outfile)
        return out_df
    except Exception as e:
        logging.error(e)
        return


# Function to Prepare a Object Name
def find_obj(data, scope):
    obj_id = data.split("\\")
    if scope == 'sys':
        return str(obj_id[4])
    else:
        return str(obj_id[3])


def main():
    pass


if __name__ == '__main__':
    main()
