from operator import index
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats

# Finds distance from normal range
def abn_dist(lab, result, norm_dict):
    norm_range = norm_dict[lab]
    low = norm_range[0] - result
    high = result - norm_range[1] # if higher, will be positive
    return low * (low > 0) + high * (high > 0)

# Computes linear regression features
def get_linreg_stats(offsets : pd.Series, results : pd.Series):
    x = offsets.values
    y = results.values
    linreg = stats.linregress(x, y)
    return float(linreg.slope), float(linreg.intercept)

def df_linreg(df):
    return get_linreg_stats(df.relativelabresultoffset, df.labresult)

# Calculates mean intervals for a single patient series
def mean_interval(df: pd.Series):
    arr = df.values
    intervals = arr[1:] - arr[:-1]
    return np.mean(intervals)

# Imputes missing features for the full patient and full lab dataframe
def impute_feat(norm_dict, final_df):
    means = final_df.groupby(['labname']).mean()
    means.fillna(0, inplace=True)
    medians = final_df.groupby(['labname']).mean()
    medians.fillna(0, inplace=True)
    imputed_df = final_df.copy().set_index('labname')
    for lab, group in final_df.groupby(['labname']):
        norm_val = np.mean(norm_dict[lab])
        # fill_dict = {feature : means.at[lab, feature] for feature in final_df.columns}
        
        group.fillna({
            'haslab' : 0,
            'last' : norm_val,
            'mean' : norm_val,
            'median' : norm_val,
            'min' : norm_val,
            'max' : norm_val,
            'var' : medians.at[lab, 'var'],
            'slope': medians.at[lab, 'slope'], 
            'intercept': norm_val, 
            'timesinceabn': medians.at[lab, 'timesinceabn'], 
            'maxabndistance': medians.at[lab, 'maxabndistance'], 
            'avgabninterval': medians.at[lab, 'avgabninterval']
            }, inplace=True)
        imputed_df.loc[lab] = group.set_index('labname')
    imputed_df.reset_index(inplace=True)
    return imputed_df

# Creates a full feature dataframe, each patient has 12 * 32 features in one row
def create_full_features(norm_dict, imputed_df):
    # Flattens into dataframe with one row per patient
    data_dict = {}
    for id, group in imputed_df.set_index(['patientunitstayid', 'labname']).groupby(['patientunitstayid']):
        data = group.to_numpy().flatten()
        data_dict[id] = data
    full_data_df = pd.DataFrame.from_dict(data_dict, orient='index')
    full_data_df.index.rename('patientunitstayid', inplace=True)
    full_data_df.reset_index(inplace=True)

    # Renames columns to useful feature names
    feat_names = list(imputed_df.columns)
    feat_names = feat_names[-12:]
    lab_names = list(norm_dict.keys()).copy()
    lab_names.sort()
    col_names = {idx: lab_names[idx // 12] + "_" + feat_names[idx % 12] for idx in range(12 * len(lab_names))}
    full_data_df.rename(col_names, axis=1, inplace=True)
    return full_data_df


# Function used for cleaning and generating new features
def extended_features(norm_dict, lead_time = 1):
    # Set up paths
    file_path = Path(__file__)
    eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')
    dataset_path = file_path.parent.parent.parent.joinpath('Dataset')

    # If computed features exist for that lead time, read feature table instead of recomputing
    if (eicu_path.joinpath(str(lead_time) + 'FeatureTable.csv').is_file()):
        final_df = pd.read_csv(eicu_path.joinpath(str(lead_time) + 'FeatureTable.csv'))
        delirium_times = pd.read_csv(dataset_path.joinpath('relative_' + str(lead_time) + 'hr_lead_1hr_obs_data_set.csv'), index_col='patientunitstayid')
        d_start_times = delirium_times['end']
        ground_truth = delirium_times['delirium?']
    else: # Compute Features:

        # read lab file, delirium time file, giving end of observation window and ground truth labels
        lab_file = pd.read_csv(eicu_path.joinpath('lab_delirium.csv'), usecols=[
            'patientunitstayid', 'labresultoffset', 'labname', 'labresult'
            ])
        delirium_times = pd.read_csv(dataset_path.joinpath('relative_' + str(lead_time) + 'hr_lead_1hr_obs_data_set.csv'), index_col='patientunitstayid')
        d_start_times = delirium_times['end']
        ground_truth = delirium_times['delirium?']

        # Drop patients and labs that are not being used for the model
        lab_file[lab_file['patientunitstayid'].isin(delirium_times.index)]
        lab_file = lab_file[lab_file['labresultoffset'] >= 0]
        lab_file = lab_file[lab_file['labname'].isin(norm_dict)]

        # find the time of each lab relative to observation end
        lab_file['deliriumstartoffset'] = lab_file['patientunitstayid'].apply(d_start_times.get)
        lab_file['relativelabresultoffset'] = lab_file['labresultoffset'] - lab_file['deliriumstartoffset']

        # remove anything after observation window end
        lab_file = lab_file[(lab_file['relativelabresultoffset'] <= 0)]

        # Now data is cleaned: ready to be analyzed
        # abndistance computes maximum distance of any lab from normal lab range
        lab_file['abndistance'] = lab_file.apply(lambda x: abn_dist(x.labname, x.labresult, norm_dict), axis=1)

        # Finds simple statistical features
        result_df = lab_file.groupby(['patientunitstayid', 'labname'])['labresult'].agg(['last', 'mean', 'var', 'median', 'min', 'max'])
        result_df.reset_index(inplace=True)

        # Finds linear regression features
        linreg_df = lab_file.groupby(['patientunitstayid','labname'])[['relativelabresultoffset', 'labresult']].apply(df_linreg)
        result_df[['slope', 'intercept']] = pd.DataFrame(linreg_df.to_list(), columns=['slope', 'intercept'])
        result_df.set_index(['patientunitstayid', 'labname'], inplace=True)

        # Filter out labs that are abnormal to extract features
        abn_labs = lab_file[lab_file['abndistance'].apply(bool)]
        abn_labs_sorted = abn_labs.sort_values([
            'patientunitstayid', 'labname', 'labresultoffset'
            ]).groupby(['patientunitstayid', 'labname'])

        # Finds most recent abnormal lab (timesinceabn), maximum distance from normal lab range (maxabndistance), 
        # and computes the mean interval (in min) between abnormal values
        abn_df = pd.concat([-abn_labs_sorted['relativelabresultoffset'].max(), 
                            abn_labs_sorted['abndistance'].max(), 
                            abn_labs_sorted['labresultoffset'].agg(mean_interval)], axis=1)
        abn_df.rename(columns={ 'relativelabresultoffset': 'timesinceabn', 
                                'abndistance': 'maxabndistance', 
                                'labresultoffset' : 'avgabninterval'}, inplace=True)
        
        # Anything without gets filled w/ 0
        abn_df.fillna({'avgabninterval': 0}, inplace=True)

        # Dataframe containing only those with clean data
        has_lab_stats = pd.concat([result_df, abn_df], axis=1)
        has_lab_stats['haslab'] = 1

        # Creates dataframe of all patients, all labs, including missing data
        final_arr = []
        for id in d_start_times.index:
            for lab in norm_dict:
                final_arr.append([id, lab])
        final_df = pd.DataFrame(final_arr, columns=['patientunitstayid', 'labname'])
        final_df = pd.concat([final_df.set_index(['patientunitstayid', 'labname']), has_lab_stats], axis=1)
        final_df.reset_index(inplace=True)
        final_df.sort_values(['patientunitstayid', 'labname'], inplace=True)

        # Writes to a full feature table for later reuse
        final_df.to_csv(eicu_path.joinpath(str(lead_time) + 'FeatureTable.csv'), index=False)
    
    # Imputes missing features
    imputed_df = impute_feat(norm_dict, final_df)

    # Creates dataframe containing all features for each patient, not divided by labs
    full_data_df = create_full_features(norm_dict, imputed_df)

    return final_df, ground_truth, imputed_df, full_data_df
