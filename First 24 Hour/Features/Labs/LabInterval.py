

# %% Package setup and loading data
from pathlib import PurePath
import os
import numpy as np
import pandas as pd

file_path = PurePath(os.getcwd())
dataset_path = file_path.parent.parent.parent.joinpath('eicu')
lab_file = pd.read_csv(dataset_path.joinpath('lab.csv'), usecols=[
    'patientunitstayid', 'labresultoffset', 'labname', 'labresultrevisedoffset'])
d_lab_file = pd.read_csv(dataset_path.joinpath('lab_delirium.csv'), usecols=[
    'patientunitstayid', 'labresultoffset', 'labname', 'labresultrevisedoffset'])


# %% Removes any negative offsets
lab_file = lab_file[lab_file['labresultoffset'] > 0]
d_lab_file = d_lab_file[d_lab_file['labresultoffset'] > 0]

# %% Creates DF containing all lab names and a dictionary to map each name to a number, allowing it to be put into a numpy array
unique_labs = lab_file['labname'].unique()
lab_index = {lab: idx for idx, lab in enumerate(unique_labs)}
mapped_lab_file = lab_file.replace(lab_index)
d_mapped_lab_file = d_lab_file.replace(lab_index)
mapped_lab_arr = mapped_lab_file.to_numpy(dtype=int)
d_mapped_lab_arr = d_mapped_lab_file.to_numpy(dtype=int)

# %% Function to package statistics from an array with its lab label
def get_stats(lab, arr):
    if np.size(arr):
        q3, q1 = np.percentile(arr, [75, 25])
        IQR = q3 - q1
    else:
        return [unique_labs[lab], np.NaN, np.NaN, np.NaN, np.NaN]
    # mean = arr.mean()
    # median = arr.median()
    # var = arr.var()
    # minima = arr.min()
    # maxima = arr.max()
    # return mean, var, median, IQR, minima, maxima
    return [unique_labs[lab], np.mean(arr), np.var(arr), np.median(arr), IQR, np.min(arr), np.max(arr)]
    

# %% Finds frequency of each lab and finds time-between-labs statistics, e.g. average time between labs of each type
freq_stats = [] # will contain lists of labs and their corresponding statistics
count_stats = []
for lab in range(158): 
    df_lab = mapped_lab_file[mapped_lab_file['labname'] == lab]
    if df_lab.empty:
        freq_stats.append(get_stats(lab, [])) # Returns name and NaN's
        continue
    
    # Groups the same ids together, then for each id, sort in ascending order by offset
    df_lab_sorted = df_lab.sort_values(by=['patientunitstayid','labresultoffset']).reset_index()
    lab_counts = df_lab_sorted['patientunitstayid'].value_counts()

    # Gets how many labs-per-patient statistics e.g. average # of labs per patient, how many patients have at least one lab
    count_stats.append([unique_labs[lab], lab_counts.mean(), lab_counts.var(), lab_counts.median(), lab_counts.sum(), len(lab_counts)])
    
    lab_counts = lab_counts.to_dict()
    arr_lab = df_lab_sorted.to_numpy(dtype=int)

    # Used to compute filters
    index_map = [0]
    length_map = []
    for i, id in enumerate(df_lab['patientunitstayid'].unique()):
        index_map.append(index_map[i] + lab_counts[id])
        length_map.append(lab_counts[id])
    index_map = index_map[:-1]
    index_m_arr = np.asarray(index_map)
    length_m_arr = np.asarray(length_map)

    # Computes interval between labs
    all_times = arr_lab[:, -3] # Time vector, -3 for 'labresultoffset', -1 for 'revisedlabresultoffset'
    time_diff = all_times[1:] - all_times[:-1]

    # Filters out non-matching subtractions and "1-lab patient ids"
    afil1 = index_m_arr - 1
    afil2 = index_m_arr[length_m_arr == 1]
    anti_filter = np.append(afil1, afil2)
    true_filter = ~np.isin(np.arange(all_times.size - 1), anti_filter)

    filtered_time_diff = time_diff[true_filter]
    filtered_time_diff = filtered_time_diff[filtered_time_diff != 0] # Removes 0 intervals

    freq_stats.append(get_stats(lab, filtered_time_diff))
freq_stats_df = pd.DataFrame(freq_stats, columns = ["lab", "mean_freq", "var_freq", "median_freq", "IQR_freq", "min_freq", "max_freq"])
count_stats_df = pd.DataFrame(count_stats, columns = ["lab", "mean_count", "var_count", "median_count", "total labs", "n patients w/ lab"])
# %% Same thing but using delirium only
d_freq_stats = []
d_count_stats = []
for lab in range(158):
    df_lab = d_mapped_lab_file[d_mapped_lab_file['labname'] == lab]
    if df_lab.empty:
        d_freq_stats.append(get_stats(lab, []))
        continue

    df_lab_sorted = df_lab.sort_values(by=['patientunitstayid','labresultoffset']).reset_index()
    lab_counts = df_lab_sorted['patientunitstayid'].value_counts()

    d_count_stats.append([unique_labs[lab], lab_counts.mean(), lab_counts.var(), lab_counts.median(), lab_counts.sum(), len(lab_counts)])
    
    lab_counts = lab_counts.to_dict()
    arr_lab = df_lab_sorted.to_numpy(dtype=int)

    index_map = [0]
    length_map = []
    for i, id in enumerate(df_lab['patientunitstayid'].unique()):
        index_map.append(index_map[i] + lab_counts[id])
        length_map.append(lab_counts[id])
    index_map = index_map[:-1]
    index_m_arr = np.asarray(index_map)
    length_m_arr = np.asarray(length_map)

    all_times = arr_lab[:, -3]
    time_diff = all_times[1:] - all_times[:-1]
    
    afil1 = index_m_arr - 1
    afil2 = index_m_arr[length_m_arr == 1]
    anti_filter = np.append(afil1, afil2)
    true_filter = ~np.isin(np.arange(all_times.size - 1), anti_filter)

    filtered_time_diff = time_diff[true_filter]
    filtered_time_diff = filtered_time_diff[filtered_time_diff != 0]

    d_freq_stats.append(get_stats(lab, filtered_time_diff))
d_freq_stats_df = pd.DataFrame(d_freq_stats, columns = ["lab", "mean_freq", "var_freq", "median_freq", "IQR_freq", "min_freq", "max_freq"])
d_count_stats_df = pd.DataFrame(d_count_stats, columns = ["lab", "mean_count", "var_count", "median_count", "total labs", "n patients w/ lab"])
# %%
freq_stats_df.to_csv(file_path.joinpath('Lab_Frequency.csv'))
d_freq_stats_df.to_csv(file_path.joinpath('Lab_Frequency_delirium.csv'))

count_stats_df.to_csv(file_path.joinpath('Lab_Count.csv'))
d_count_stats_df.to_csv(file_path.joinpath('Lab_Count_delirium.csv'))


