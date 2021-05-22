import pandas as pd
import scipy.stats as st

def ta_chip(high, low, close, volume, window):
    price_dist = (close-(high+low)/2)**2
    new_vol = price_dist * volume
    chip_avg = pd.Series(np.nan, index=close.index)
    chip_score = pd.Series(np.nan, index=close.index)
    #
    for i in range(window-1, close.shape[0]):
        newvol_col = new_vol.iloc[i-window+1:i+1]
        weight = newvol_col / newvol_col.sum()
        price_weight = close.iloc[i-window+1:i+1] * weight
        chip_avg.iloc[i] = price_weight.sum()
        z_score = (close.iloc[i] - chip_avg.iloc[i]) / close.iloc[i-window+1:i+1].std()
        p_values = st.norm.cdf(z_score)
        chip_score.iloc[i] = p_values
    #
    return chip_avg, chip_score