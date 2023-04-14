"""Program to train the Light Gradient Boost Model

# Use engineered features to train a model
This code will use the features created to train a Light Gradient Boost Model (lgbm).
The process is:
1. Run a lgbm model and compare it to a base model
"""

# Import libraries
import pandas as pd
import numpy as np
import datetime as dt
import datetime as dt

import lightgbm as lgb
from sklearn.model_selection import RandomizedSearchCV
from sklearn.model_selection import GridSearchCV

from stock_trading_ml_modelling.config import FT_ENG_W, FT_ENG_COL_LIST, \
    TARGET_COL, LGBM_REM_INF, DATE_LIM, RAND_SEED, CUSTOM_METRIC, \
    LGBM_FIXED_PARAMS, LGBM_SEARCH_PARAMS, LGBM_FIT_PARAMS, \
    LGBM_USE_CUST_EVAL_SET, LGBM_USE_CUST_LOSS_FUNC, CUSTOM_METRIC
from stock_trading_ml_modelling.utils.timing import ProcessTime
from stock_trading_ml_modelling.utils.measure import measure_acc

# Import and combine prices files
df_ft = pd.read_hdf(FT_ENG_W)
print("SHAPE: {}".format(df_ft.shape))
print(df_ft.dtypes)
print(df_ft.head())

###########################
### BUILD THE LGB MODEL ###
###########################
# This model is designed to predict if a week should be buy, hold or sell.

# Import the feature cols
with open(FT_ENG_COL_LIST) as f:
    feature_cols = f.read().split(',')
for col in ['signal', 'ticker']:
    try:
        feature_cols.remove(col)
    except:
        print('{} DOES NOT EXIST'.format(col))
print('feature_cols length -> {}'.format(len(feature_cols)))
print('feature_cols -> {}'.format(feature_cols))

df_model = df_ft[feature_cols+[TARGET_COL]+['date']].copy()
print("DTYPES:", df_model.dtypes)
print("SHAPE:", df_model.shape)
print('{} UNIQUE SIGNALS -> {}'.format(
    len(np.unique(df_model[TARGET_COL])), np.unique(df_model[TARGET_COL])))

# Remove rows with missing or infinate values
if LGBM_REM_INF:
    print('DROP NAS')
    df_model.dropna(inplace=True)
    df_model.reset_index(inplace=True)
    print("DTYPES:", df_model.dtypes)
    print("SHAPE:", df_model.shape)


## SETUP TEST TRAIN DATASETS ###
# Create the train and test dataset
# All prices pre-2014 are training, all post 2014 are testing
# This remove the posibility that we are learning from other shares at times what is good/bad

# Separate into test and train
df_train = df_model[df_model.date < DATE_LIM]
df_test = df_model[df_model.date >= DATE_LIM]

# Reset the index and drop 'date' column
df_train = df_train.reset_index(drop=True).drop(columns=['date'])
df_test = df_test.reset_index(drop=True).drop(columns=['date'])

print("train rows: {:,}".format(df_train.shape[0]))
print("test rows: {:,}".format(df_test.shape[0]))

# Shuffle the datasets
np.random.seed(RAND_SEED)
rand_index = np.random.permutation(df_train.index.values)
df_train = df_train.iloc[rand_index].reset_index(drop=True)
rand_index = np.random.permutation(df_test.index.values)
df_test = df_test.iloc[rand_index].reset_index(drop=True)


### SEPARATE INTO FEATURES AND TARGETS ###
# Test
X_test = df_test[feature_cols]
y_test = df_test[TARGET_COL]
print('X_test.shape -> {}'.format(X_test.shape))
print('y_test.shape -> {}'.format(y_test.shape))


### SPLIT TRAIN INTO TRAIN AND VALIDATION ###
# Train
X_train = df_train[feature_cols]
y_train = df_train[TARGET_COL]
print('X_train.shape -> {}'.format(X_train.shape))
print('y_train.shape -> {}'.format(y_train.shape))
# index_lim = int(np.floor(df_train.shape[0] * 0.8))
# #Train
# X_train = df_train[feature_cols].iloc[:index_lim]
# y_train = df_train[target_cols].iloc[:index_lim]
# print('X_train.shape -> {}'.format(X_train.shape))
# print('y_train.shape -> {}'.format(y_train.shape))
# #Validation
# X_valid = df_train[feature_cols].iloc[index_lim:]
# y_valid = df_train[target_cols].iloc[index_lim:]
# print('X_valid.shape -> {}'.format(X_valid.shape))
# print('y_valid.shape -> {}'.format(y_valid.shape))


### CREATE BASE LEVEL MODELS ###

# Function to calculate the True Positive Rate for each classifier
def calc_tpr(pred_signal_s, real_signal_s, features=[], unique_classes=['buy', 'sell', 'hold']):
    tpr_li = []
    for cl in unique_classes:
        tpr_li.append(measure_acc(pred_signal_s, real_signal_s,
                      features, _opt_text=cl, _multiclass=True, _cl=cl))
    cols = [x for x in tpr_li[0]]
    tpr_df = pd.DataFrame(tpr_li, columns=cols)
    return tpr_df


unique_classes = y_test.unique()
for signal in unique_classes:
    print(calc_tpr(np.full((1, y_test.shape[0]), signal)[0], y_test, [
          "ALL {}".format(str(signal).upper())], unique_classes))


### CREATE A CLASSIFICATION MODEL - LGBM ###
# The model will take a one-vs-all approach (IE 1 if it is this value, 0 if it is anything else) using the variables of "buy", "hold", and "sell" individually and building a model to find the error rate on each one.

# Error rate is determined by the four classifications:
# - True positive - correct - model is 1, actual is 1
# - True negative - correct - model is 0, actual is 0
# - False positive - error - model is 1, actual is 0
# - False negative - error - model is 0, actual is 1

# Build a custom loss function
def lgbm_custom_loss(_y_act, _y_pred):
    # Convert _y_pred into classes
    _y_pred_conv = []
    _n_classes = len(np.unique(_y_act))
    _binary = _n_classes == 2 and 1 in np.unique(
        _y_act) and 0 in np.unique(_y_act)
    if _y_act.shape == _y_pred.shape:
        if _binary:
            _y_pred = np.round(_y_pred)
        _y_pred_conv = _y_pred
    else:
        for i in range(0, _y_act.shape[0]):
            _tmp_li = []
            for j in range(0, _n_classes):
                _tmp_li.append(_y_pred[(_y_act.shape[0]*j) + i])
            _y_pred_conv.append(np.argmax(_tmp_li))
    _y_pred_conv = np.array(_y_pred_conv)
    _ac_results = calc_tpr(_y_pred_conv, _y_act,
                           unique_classes=list(np.unique(_y_act)))
    # _av = _ac_results[_ac_results.opt_text.isin([0,2])]['ppv'].mean() #Only average for buy and sell
    if _binary:
        _av = _ac_results[_ac_results.opt_text == 1][CUSTOM_METRIC].values[0]
    else:
        _av = _ac_results[CUSTOM_METRIC].mean()
    # Append the time to prevent early stopping
    if (_av == 0 and CUSTOM_METRIC != 'auc') or (_av == 0.5 and CUSTOM_METRIC == 'auc'):
        _time_now = dt.datetime.now()
        _av += _time_now.hour*10**-4 + _time_now.minute*10**-6 + _time_now.second*10**-8
    # (eval_name, eval_result, is_higher_better)
    return 'lgbm_custom_loss', _av, True


# Setup variables
mod_fixed_params = LGBM_FIXED_PARAMS
# mod_fixed_params['num_class'] = len(np.unique(y_train))
mod_fixed_params['num_class'] = 1
search_params = LGBM_SEARCH_PARAMS
fit_params = LGBM_FIT_PARAMS
if LGBM_USE_CUST_EVAL_SET:
    # fit_params['eval_set'] = (X_valid,y_valid)
    fit_params['eval_set'] = (X_test, y_test)
    fit_params['early_stopping_rounds'] = 5
    fit_params['eval_metric'] = 'auc'
if LGBM_USE_CUST_LOSS_FUNC:
    fit_params['eval_metric'] = lgbm_custom_loss

# Train the model
run_time = ProcessTime()
# Find the minimum using skopt
skopt_params = [
    space.Real(0.01,
               0.5, name="learning_rate", prior="logger-uniform"), space.Integer(1,
                                                                                 30, name="max_depth"), space.Integer(2,
                                                                                                                      100, name="num_leaves"), space.Integer(200,
                                                                                                                                                             2000, name="min_samples_split"), space.Integer(50,
                                                                                                                                                                                                            500, name="min_samples_leaf"),
]
# @skopt.utils.use_named_args(CONFIG['lgbm_training']['skopt_params'])
# REPLACE SKOPT WITH HYPEROPT


def train_func(**params):
    print('\n')
    lgb_mod = lgb.LGBMClassifier(**params)
    best_mod = lgb_mod.fit(X_train, y_train, **fit_params)
    y_pred = best_mod.predict(X_test)
    print('y_pred.value_counts() -> \n{}'.format(np.unique(y_pred, return_counts=True)))
    loss = lgbm_custom_loss(y_test, y_pred)
    if loss[2] == True:  # higher is better
        loss = 1 - loss[1]
    else:
        loss = loss[1]
    print('loss -> {}'.format(loss))
    return loss


HPO_PARAMS = {
    'n_calls': 100
}
# results = skopt.gbrt_minimize(train_func, **HPO_PARAMS)
# print('\nBEST RESULT - {} -> {:.4f}'.format(CUSTOM_METRIC, results.fun))
# best_params = {k.name: v for k, v in zip(
#     CONFIG['lgbm_training']['skopt_params'], results.x)}
# print('\nBEST PARAMS -> {}'.format(results.x))
# run_time.end()

# ### FIT THE FINAL MODEL USIN BEST PARAMS ###
# final_models = lgb.LGBMClassifier(**best_params)
# final_models.fit(X_train, y_train)

# # Show the best and worst features
# lgb.plot_importance(final_models, figsize=(12, 36))

# # Run on the testing set
# y_pred = final_models.predict(X_test)
# print(calc_tpr(y_pred, y_test, [
#       'FINAL RF MODEL - TEST SET'], y_train.unique()))

# # Run on the training set
# y_pred = final_models.predict(X_train)
# print(calc_tpr(y_pred, y_train, [
#       'FINAL RF MODEL - TRAIN SET'], y_train.unique()))


# ### EXPORT THE MODEL ###
# # Export the model
# jl.dump(final_models, LGB_MODEL)

# # Export a list of the features for this model
# file_object = open(LGB_MODEL_FEATURE_LIST, 'w')
# feature_str = ''
# for i in feature_cols:
#     feature_str += '{},'.format(i)
# feature_str = feature_str[:-1]
# file_object.write(feature_str)
# file_object.close()
# print('feature_str -> \n{}'.format(feature_str))
