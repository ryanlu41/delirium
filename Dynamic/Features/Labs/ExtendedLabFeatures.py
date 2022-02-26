# %% [markdown]
# ## TODO:
# ### Generating Features:
# - ~~binary indicator (has it or doesn't)~~
# - ~~mean, min, max, std, med, furthest distance from normal ~~
# - ~~num of min since abn~~
# - ~~freq of abn~~
# - ~~linear regression of labs~~
#     - ~~use for past labs too~~
# ### ~~break into different modules~~
# 

#%% Package setup.
import numpy as np
import pandas as pd
#import multiprocessing as mp
from pathlib import Path
import matplotlib.pyplot as plt
from sklearn import linear_model, metrics, model_selection, ensemble, feature_selection
from GenerateExtendedFeat import extended_features
from joblib import dump, load
# %%
file_path = Path(__file__)
eicu_path = file_path.parent.parent.parent.parent.joinpath('eicu')
model_path = file_path.parent.parent.parent.joinpath('Modeling')
# %%
lab_list_path = file_path.parent.joinpath('LabNormList.csv')
full_lab_list = pd.read_csv(lab_list_path)
full_lab_dict = full_lab_list.set_index('labname').dropna().sort_index().to_dict('index')
norm_dict = {labname : (full_lab_dict[labname][' low'], full_lab_dict[labname][' high']) for labname in full_lab_dict}

# %%
final_df, ground_truth, imputed_df, full_data_df = extended_features(norm_dict, 1)

# full_data_df = create_full_features(norm_dict, imputed_df.drop('timesinceabn', axis=1)) used for testing without timesinceabn

# %% Train/Test full model
full_metrics_arr = []
X = full_data_df.copy().reset_index()
Y = X['patientunitstayid'].apply(ground_truth.get)
X.drop('patientunitstayid', axis=1)
kf = model_selection.KFold(n_splits=5)
for train_index, test_index in kf.split(X):
    metrics_vec = ['full']
    X_train, X_test = X.iloc[train_index], X.iloc[test_index]
    Y_train, Y_test = Y.iloc[train_index], Y.iloc[test_index]

    clf = linear_model.LogisticRegression().fit(X_train, Y_train)
    LR_train_AUC = metrics.roc_auc_score(Y_train, clf.decision_function(X_train))
    LR_test_AUC = metrics.roc_auc_score(Y_test, clf.decision_function(X_test))

    fgbm = ensemble.GradientBoostingClassifier().fit(X_train, Y_train)

    Y_train_scores = fgbm.decision_function(X_train)
    fpr, tpr, thresholds = metrics.roc_curve(Y_train, Y_train_scores)
    train_AUC = metrics.roc_auc_score(Y_train, Y_train_scores)

    Y_test_scores = fgbm.decision_function(X_test)
    fpr, tpr, thresholds = metrics.roc_curve(Y_test, Y_test_scores)
    test_AUC = metrics.roc_auc_score(Y_test, Y_test_scores)
    
    metrics_vec.extend([LR_train_AUC, LR_test_AUC, train_AUC, test_AUC])
    full_metrics_arr.append(metrics_vec)

# plt.plot(fpr, tpr)
# %%
full_metrics = pd.DataFrame(full_metrics_arr, columns=['labname', 'LR_train_AUC', 'LR_test_AUC', 'GBM_train_AUC', 'GBM_test_AUC'])

# %% Train models for each lab
metrics_arr = []
for lab in norm_dict:
    lab_df = imputed_df[imputed_df['labname'] == lab]
    X = lab_df.drop(columns='labname')
    Y = X['patientunitstayid'].apply(ground_truth.get) # might not need, more of an insurance
    X.drop('patientunitstayid', axis=1)
    kf = model_selection.KFold(n_splits=5)
    for train_index, test_index in kf.split(X):
        metrics_vec = [lab]
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        Y_train, Y_test = Y.iloc[train_index], Y.iloc[test_index]

        clf = linear_model.LogisticRegression().fit(X_train, Y_train)
        LR_train_AUC = metrics.roc_auc_score(Y_train, clf.decision_function(X_train))
        LR_test_AUC = metrics.roc_auc_score(Y_test, clf.decision_function(X_test))

        gbm = ensemble.GradientBoostingClassifier().fit(X_train, Y_train)
        train_AUC = metrics.roc_auc_score(Y_train, gbm.decision_function(X_train))
        test_AUC = metrics.roc_auc_score(Y_test, gbm.decision_function(X_test))
        
        metrics_vec.extend([LR_train_AUC, LR_test_AUC, train_AUC, test_AUC])
        metrics_arr.append(metrics_vec)

# # %%
# X = delirium_times['start','end']
# Y = delirium_times['delirium?']
# kf = model_selection.KFold(n_splits=5)
# for train_index, test_index in kf.split(X):
#     X_train, X_test = X.iloc[train_index], X.iloc[test_index]
#     Y_train, Y_test = Y.iloc[train_index], Y.iloc[test_index]

#     gbm = ensemble.GradientBoostingClassifier().fit(X_train, Y_train)
#     Y_train_scores = gbm.decision_function(X_train)
#     fpr, tpr, thresholds = metrics.roc_curve(Y_train, Y_train_scores)
#     train_AUC = metrics.auc(fpr, tpr)

#     Y_test_scores = gbm.decision_function(X_test)
#     fpr, tpr, thresholds = metrics.roc_curve(Y_test, Y_test_scores)
#     test_AUC = metrics.auc(fpr, tpr)
#     test_acc = metrics.accuracy_score(Y_test, gbm.predict(X_test))

# %%
metrics_df = pd.DataFrame(metrics_arr, columns=['labname', 'LR_train_AUC', 'LR_test_AUC', 'GBM_train_AUC', 'GBM_test_AUC'])
mean_metrics = metrics_df.groupby('labname').mean()
mean_metrics = mean_metrics.append(full_metrics.mean().rename('full'))
mean_metrics.sort_values('GBM_test_AUC', ascending=False, inplace=True)
mean_metrics[['GBM_test_AUC', 'LR_test_AUC', ]].plot.bar()

# %%
dump(fgbm, model_path.joinpath('full_gbm.joblib'))
# %%
