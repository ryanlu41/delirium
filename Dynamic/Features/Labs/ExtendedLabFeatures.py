# %% [markdown]
# ## TODO:
# ### Generating Features:
# - binary indicator (has it or doesn't)
# - mean, min, max, std, med, furthest distance from normal, 
# - num of min since abn
# - freq of abn
# - linear regression of labs
#     - use for past labs too
# - break into different modules

#%% Package setup.
import numpy as np
import pandas as pd
import os
#import multiprocessing as mp
from pathlib import Path
import matplotlib.pyplot as plt
import time
from scipy import stats
from sklearn import linear_model, metrics, model_selection, ensemble
from sklearn import model_selection

# %%
def abn_dist(lab, result, norm_dict):
    norm_range = norm_dict[lab]
    low = norm_range[0] - result
    high = result - norm_range[1] # if higher, will be positive
    return low * (low > 0) + high * (high > 0)

def get_linreg_stats(offsets : pd.Series, results : pd.Series):
    x = offsets.values
    y = results.values
    linreg = stats.linregress(x, y)
    return float(linreg.slope), float(linreg.intercept)

def df_linreg(df):
    return get_linreg_stats(df.relativelabresultoffset, df.labresult)

def mean_interval(df: pd.Series):
    arr = df.values
    intervals = arr[1:] - arr[:-1]

    return np.mean(intervals)

# %%
file_path = Path(__file__)
eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')

# %%
lab_list_path = file_path.parent.joinpath('LabNormList.csv')
full_lab_list = pd.read_csv(lab_list_path)


# %%
lead_time = 1 #hrs
# obs_hours = 24 #hrs
lead_offset = -60 * lead_time
# obs_offset = -60 * obs_hours

# %%
full_lab_dict = full_lab_list.set_index('labname').dropna().to_dict('index')
used_norm = {labname : (full_lab_dict[labname][' low'], full_lab_dict[labname][' high']) for labname in full_lab_dict}
unused_norm = {
    'triglycerides' : (0, 150),
    'TSH' : (0.5, 5.0),
    'total cholesterol' : (150, 200),
    'HDL' : (40, 1000),
    # 'TV' : (0, 0),
    # 'Vancomycin - trough' : (0, 0),
    'BNP' : (0, 100),
    'LDL' : (0, 130),
    'ammonia' : (40, 80),
    'prealbumin' : (18, 45),
    'free T4' : (0.9, 2.4),
    'cortisol' : (3, 20),
    # 'Digoxin' : (0, 0),
    'T4' : (5, 12),
    'CRP-hs' : (0, 0.2),
    'T3' : (70, 195),
    # 'Clostridium difficile toxin A+B' : (0, 0)
}
norm_dict = {
}
norm_dict.update(used_norm)
norm_dict.update(unused_norm)

# %% [markdown]
# - triglycerides: < 150 mg/dL (Mayo Clinic)
# - TSH: 0.5 - 5.0 mIU/L
# - total cholesterol: 150 - 200 mg/dL
# - HDL: >= 40 mg/dL
# - TV: 
# - Vancomycin - trough: 
# - Temperature: 
# - bands: 0 - 5 %
# - BNP: < 100 pg/mL
# - LDL: < 130 mg/dL
# - ammonia: 40 - 80 mcg/dL
# - prealbumin: 18 - 45 mg/dL
# - free T4: 0.9 - 2.4 ng/dL
# - cortisol: 3 - 20 mcg/dL if not given time
# - Digoxin: 
# - T4: 5 - 12 mcg/dL
# - CRP-hs: < 0.2 mg/dL
# - T3: 70 - 195 ng/dL
# - Clostridium difficile toxin A+B: 
# %%
if (eicu_path.joinpath('FeatureTable.csv').is_file()):
    final_df = pd.read_csv(eicu_path.joinpath('FeatureTable.csv'))
    d_start_times = pd.read_csv(eicu_path.joinpath('DeliriumStartTimes.csv'), index_col='patientunitstayid')
    ground_truth = d_start_times['deliriumstartoffset'] >= 0
    ground_truth.fillna(0)
else:
    lab_file = pd.read_csv(eicu_path.joinpath('lab_delirium.csv'), usecols=[
        'patientunitstayid', 'labresultoffset', 'labname', 'labresult'
        ])

    d_start_times = pd.read_csv(eicu_path.joinpath('DeliriumStartTimes.csv'), index_col='patientunitstayid')
    ground_truth = d_start_times['deliriumstartoffset'] >= 0
    ground_truth.fillna(0)

    filled_start = d_start_times['deliriumstartoffset'].fillna(10000)


    lab_file = lab_file[lab_file['labresultoffset'] >= 0]
    lab_file = lab_file[lab_file['labname'].isin(norm_dict)]

    lab_file['deliriumstartoffset'] = lab_file['patientunitstayid'].apply(filled_start.get)
    lab_file['relativelabresultoffset'] = lab_file['labresultoffset'] - lab_file['deliriumstartoffset'] + lead_offset


    lab_file = lab_file[(lab_file['relativelabresultoffset'] <= 0)]
    # lab_file = lab_file[(lab_file['relativelabresultoffset'] >= obs_offset) * (lab_file['relativelabresultoffset'] <= 0)]
    lab_file['abndistance'] = lab_file.apply(lambda x: abn_dist(x.labname, x.labresult, norm_dict), axis=1)

    result_df = lab_file.groupby(['patientunitstayid', 'labname'])['labresult'].agg(['last', 'mean', 'var', 'median', 'min', 'max'])
    result_df.reset_index(inplace=True)
    linreg_df = lab_file.groupby(['patientunitstayid','labname'])[['relativelabresultoffset', 'labresult']].apply(df_linreg)
    result_df[['slope', 'intercept']] = pd.DataFrame(linreg_df.to_list(), columns=['slope', 'intercept'])
    result_df.set_index(['patientunitstayid', 'labname'], inplace=True)

    abn_labs = lab_file[lab_file['abndistance'].apply(bool)]
    abn_labs_sorted = abn_labs.sort_values([
        'patientunitstayid', 'labname', 'labresultoffset'
        ]).groupby(['patientunitstayid', 'labname'])
    abn_df = pd.concat([-abn_labs_sorted['relativelabresultoffset'].max(), abn_labs_sorted['abndistance'].max(), abn_labs_sorted['labresultoffset'].agg(mean_interval)], axis=1)
    abn_df.rename(columns={'relativelabresultoffset': 'timesinceabn', 'abndistance': 'maxabndistance', 'labresultoffset' : 'avgabninterval'}, inplace=True)
    # abn_df.reset_index(inplace=True)
    abn_df.fillna({'avgabninterval': 0}, inplace=True)



    has_lab_stats = pd.concat([result_df, abn_df], axis=1)

    has_lab_stats['haslab'] = 1

    # imputed_df = has_lab_stats.copy().set_index('labname')
    # for lab, group in has_lab_stats.groupby('labname'):
    final_arr = []
    for id in d_start_times.index:
        for lab in norm_dict:
            final_arr.append([id, lab])
    final_df = pd.DataFrame(final_arr, columns=['patientunitstayid', 'labname'])
    final_df = pd.concat([final_df.set_index(['patientunitstayid', 'labname']), has_lab_stats], axis=1)
    final_df.reset_index(inplace=True)

    final_df.to_csv(eicu_path.joinpath('FeatureTable.csv'))

# %%
means = final_df.groupby(['labname']).mean()
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
        'var' : means.at[lab, 'var'],
        'slope': means.at[lab, 'slope'], 
        'intercept': norm_val, 
        'timesinceabn': means.at[lab, 'timesinceabn'], 
        'maxabndistance': means.at[lab, 'maxabndistance'], 
        'avgabninterval': means.at[lab, 'avgabninterval']
        }, inplace=True)
    imputed_df.loc[lab] = group.set_index('labname')
imputed_df.reset_index(inplace=True)

# %%
data_dict = {}
for id, group in imputed_df.set_index(['patientunitstayid', 'labname']).groupby(['patientunitstayid']):
    data = group.to_numpy().flatten()
    data_dict[id] = data
full_data_df = pd.DataFrame.from_dict(data_dict, orient='index')
full_data_df.index.rename('patientunitstayid', inplace=True)
full_data_df.reset_index(inplace=True)

# %%
full_metrics_arr = []
X = full_data_df.copy()
Y = X['patientunitstayid'].apply(ground_truth.get)
X.set_index('patientunitstayid', inplace=True)
if sum(Y) > 1:
    kf = model_selection.KFold(n_splits=5)
    for train_index, test_index in kf.split(X):
        metrics_vec = ['full']
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        Y_train, Y_test = Y.iloc[train_index], Y.iloc[test_index]

        clf = linear_model.LogisticRegression(max_iter=300, C=0.02).fit(X_train, Y_train)
        Y_train_scores = clf.decision_function(X_train)
        fpr, tpr, thresholds = metrics.roc_curve(Y_train, Y_train_scores)
        train_AUC = metrics.auc(fpr, tpr)

        Y_test_scores = clf.decision_function(X_test)
        fpr, tpr, thresholds = metrics.roc_curve(Y_test, Y_test_scores)
        test_AUC = metrics.auc(fpr, tpr)
        test_acc = metrics.accuracy_score(Y_test, clf.predict(X_test))

        metrics_vec.extend([train_AUC, test_AUC, test_acc])

        gbm = ensemble.GradientBoostingClassifier().fit(X_train, Y_train)
        # pregbm = ensemble.GradientBoostingClassifier()
        # parameters = 
        # gbm = model_selection.GridSearchCV(pregbm, parameters).fit(X_train, Y_train)
        Y_train_scores = gbm.decision_function(X_train)
        fpr, tpr, thresholds = metrics.roc_curve(Y_train, Y_train_scores)
        train_AUC = metrics.auc(fpr, tpr)

        Y_test_scores = gbm.decision_function(X_test)
        fpr, tpr, thresholds = metrics.roc_curve(Y_test, Y_test_scores)
        test_AUC = metrics.auc(fpr, tpr)
        test_acc = metrics.accuracy_score(Y_test, gbm.predict(X_test))
        
        metrics_vec.extend([train_AUC, test_AUC, test_acc])
        full_metrics_arr.append(metrics_vec)
else:
    full_metrics_arr.append(['full', np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN])

plt.plot(fpr, tpr)
# %%
full_metrics = pd.DataFrame(full_metrics_arr, columns=['labname', 'LR_train_AUC', 'LR_test_AUC', 'LR_test_acc', 'GBM_train_AUC', 'GBM_test_AUC', 'GBM_test_acc'])

# %%
metrics_arr = []
for lab in norm_dict:
    lab_df = imputed_df[imputed_df['labname'] == lab]
    X = lab_df.drop(columns='labname')
    Y = X['patientunitstayid'].apply(ground_truth.get) # might not need, more of an insurance
    X.set_index('patientunitstayid', inplace=True)
    if sum(Y) > 1:
        kf = model_selection.KFold(n_splits=5)
        for train_index, test_index in kf.split(X):
            metrics_vec = [lab]
            X_train, X_test = X.iloc[train_index], X.iloc[test_index]
            Y_train, Y_test = Y.iloc[train_index], Y.iloc[test_index]

            clf = linear_model.LogisticRegression(max_iter=300, C=0.02).fit(X_train, Y_train)
            Y_train_scores = clf.decision_function(X_train)
            fpr, tpr, thresholds = metrics.roc_curve(Y_train, Y_train_scores)
            train_AUC = metrics.auc(fpr, tpr)

            Y_test_scores = clf.decision_function(X_test)
            fpr, tpr, thresholds = metrics.roc_curve(Y_test, Y_test_scores)
            test_AUC = metrics.auc(fpr, tpr)
            test_acc = metrics.accuracy_score(Y_test, clf.predict(X_test))

            metrics_vec.extend([train_AUC, test_AUC, test_acc])

            gbm = ensemble.GradientBoostingClassifier().fit(X_train, Y_train)
            Y_train_scores = gbm.decision_function(X_train)
            fpr, tpr, thresholds = metrics.roc_curve(Y_train, Y_train_scores)
            train_AUC = metrics.auc(fpr, tpr)

            Y_test_scores = gbm.decision_function(X_test)
            fpr, tpr, thresholds = metrics.roc_curve(Y_test, Y_test_scores)
            test_AUC = metrics.auc(fpr, tpr)
            test_acc = metrics.accuracy_score(Y_test, gbm.predict(X_test))
            
            metrics_vec.extend([train_AUC, test_AUC, test_acc])
            metrics_arr.append(metrics_vec)
    else:
        metrics_arr.append([lab, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN, np.NaN])

# %%
metrics_df = pd.DataFrame(metrics_arr, columns=['labname', 'LR_train_AUC', 'LR_test_AUC', 'LR_test_acc', 'GBM_train_AUC', 'GBM_test_AUC', 'GBM_test_acc'])
mean_metrics = metrics_df.groupby('labname').mean()
mean_metrics = mean_metrics.append(full_metrics.mean().rename('full'))

# %%
mean_metrics.sort_values('GBM_test_AUC', ascending=False, inplace=True)
mean_metrics[['GBM_test_AUC', 'LR_test_AUC', ]].plot.bar()
mean_metrics

# %%
