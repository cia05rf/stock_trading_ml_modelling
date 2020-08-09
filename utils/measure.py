from sklearn.metrics import roc_auc_score

#Get measures of success
def measure_acc(_s_pred,_s_act,_ft_cols:list=[],_opt_text:str='',_multiclass:bool=False,_cl=None,_verbose:bool=False):
    """Function creating key statistical accuracy metrics 
    by comparing predicted and actual series
    -----
    args:
        _s_pred - Pandas Series - The values predicted
        _s_act - Pandas Series - The actual values
        _ft_cols - list - A list of feature columns used
        _opt_text - str - A string used to add comments
        _verbose - bool - True - Do you want to see the results?
        _multiclass - bool - False - If true then perform a one vs many comparisson
        _cl - None - The class which is being investigated here
    returns:
        dict - {feature,tot_p,tot_n,tpr,tnr,ppv,npv,acc,auc}
    """
    #Check type of classification
    if _multiclass == False:
        _true_val = True
    else:
        if _cl == None:
            raise ValueError('The value of _cl can not be None in a multiclassification situation - please provide a value for _cl')
        else:
            _true_val = _cl
    _tot_p = len(_s_pred[_s_pred == _true_val])
    _tot_n = len(_s_pred[_s_pred != _true_val])
    #Get _tpr, _tnr, _ppv, _npv, and _acc
    _tp = len(_s_pred[(_s_pred == _true_val) & (_s_act == _true_val)]) #True positive
    _fp = len(_s_pred[(_s_pred == _true_val) & (_s_act != _true_val)]) #False positive
    _tn = len(_s_pred[(_s_pred != _true_val) & (_s_act != _true_val)]) #True negative
    _fn = len(_s_pred[(_s_pred != _true_val) & (_s_act == _true_val)]) #False negative
    _tpr = _tp / (_tp+_fn) if (_tp+_fn) > 0 else 0
    _tnr = _tn / (_tn+_fp) if (_tn+_fp) > 0 else 0
    _ppv = _tp / _tot_p if _tot_p > 0 else 0
    _npv = _tn / _tot_n if _tot_n > 0 else 0
    _acc = (_tp+_tn) / (_tot_p+_tot_n) if _tot_n + _tot_n > 0 else 0
    #Get roc_auc_score
    _bool_act_s = _s_act == _true_val
    if len(np.unique(_bool_act_s)) != 2: #IE nothing is correct
        _bool_pred_s = _s_pred == _true_val
        _auc = roc_auc_score(_bool_act_s,_bool_pred_s)
    else:
        _auc = 0.5
    if _verbose == True:
        print("\tbool counts act {} -> \n\t\ttrue count:{:,}, false count:{:,}, true %:{:.2f}".format(_opt_text,len(_s_act[_s_act == True]),len(_s_act[_s_act == False]),100*len(_s_act[_s_act == True])/len(_s_act)))
        print("\tbool counts pred {} -> \n\t\ttrue count:{:,}, false count:{:,}, true %:{:.2f}".format(_opt_text,_tot_p,_tot_n,100*len(_s_pred[_s_pred == True])/len(_s_pred)))
        print("\tdetails {} -> \n\t\t_tp:{:,}, _fp:{:,}, _tn:{:,}, _fn:{:,}".format(_opt_text,_tp,_fp,_tn,_fn))
        print("\tsummary {} -> \n\t\t_tpr:{:.4f}, _tnr:{:.4f}, _ppv:{:.4f}, _npv:{:.4f}, _acc:{:.4f}, _auc:{:.4f}".format(_opt_text,_tpr,_tnr,_ppv,_npv,_acc,_auc))
    return {
        "feature":_ft_cols.copy()
        ,"tot_p":_tot_p
        ,"tot_n":_tot_n
        ,"tpr":_tpr
        ,"tnr":_tnr
        ,"ppv":_ppv
        ,"npv":_npv
        ,"acc":_acc
        ,"auc":_auc
        ,"opt_text":_opt_text
    }