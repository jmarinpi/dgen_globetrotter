# -*- coding: utf-8 -*-
"""
Module contains functions to calculate the bass diffusion of distributed generation adoption.

On an agent-level basis :py:mod:`diffusion_functions`:
    (1) Determines maximum market size as a function of payback time
    (2) Parameterizes the Bass diffusion curve with diffusion rates (p, q) set by payback time
    (3) Determines the current stage (equivaluent time) of diffusion based on existing market and current economics 
    (4) Calculates the new market share by stepping forward on diffusion curve.
"""

import numpy as np
import pandas as pd
import utility_functions as utilfunc
import decorators
import config

#==============================================================================
# Load logger
logger = utilfunc.get_logger()
#==============================================================================

#=============================================================================
# ^^^^  Diffusion Calculator  ^^^^
@decorators.fn_timer(logger = logger, tab_level = 2, prefix = '')
def calc_diffusion_solar(df, is_first_year, bass_params, override_p_value = None, override_q_value = None, override_teq_yr1_value = None):
    """
    Calculates the market share (ms) added in the solve year.
    
    Market share must be less than max market share (mms) except  when initial ms is greater than the calculated mms.
    For this circumstance, no diffusion allowed until mms > ms. Also, do not allow ms to decrease if economics deterioriate.
    Using the calculated market share, relevant quantities are updated.

    Parameters
    ----------
    df : pandas.DataFrame
        Input dataframe.
    is_first_year : bool
        Passed to :func:`diffusion_functions.calc_diffusion_market_share` to determine the increment of `teq`
    bass_params : pandas.DataFrame
        DataFrame generally derived from :func:`settings.get_bass_params`, includes the following attributes: config.BA_COLUMN, `sector_abbr`, `state_id`, `p`, `q`, `teq_yr1`, `tech`.
    override_p_values : float , optional
        Value to override bass diffusion `p` coefficient of innovation with.
    overide_q_values : float, optional
        Value to override bass diffusion `q` coefficient of immitation with.
    override_teq_yr1_value : float, optional
        Value to override bass diffusion `teq_yr1` value representing the number of years since diffusion began for the first year of observation.

    Returns
    -------    
    pandas.DataFrame
            Dataframe contains `market_last_year` column to inform diffusion in next year.

    """
    df = df.reset_index()

    bass_params = bass_params[bass_params['tech']=='solar']    
    
    # set p/q/teq_yr1 params  
    bass_params = bass_params[[config.BA_COLUMN,'sector_abbr', 'p', 'q', 'teq_yr1']]  
    df = pd.merge(df, bass_params, how = 'left', on  = [config.BA_COLUMN,'sector_abbr','state_id'])
    print(('diffusion_functions line 60', df.shape))
    
    # calc diffusion market share
    df = calc_diffusion_market_share(df, is_first_year)
    
    # market share floor is based on last year's market share
    df['market_share'] = np.maximum(df['diffusion_market_share'], df['market_share_last_year'])
   
    # calculate the "new" market share (old - current)
    df['new_market_share'] = df['market_share'] - df['market_share_last_year']

    # cap the new_market_share where the market share exceeds the max market share
    df['new_market_share'] = np.where(df['market_share'] > df['max_market_share'], 0, df['new_market_share'])

    # calculate new adopters, capacity and market value            
    df['new_adopters'] = df['new_market_share'] * df['developable_customers_in_bin']
    df['new_market_value'] = df['new_adopters'] * df['pv_kw'] * df['pv_price_per_kw']

    df['new_pv_kw'] = df['new_adopters'] * df['pv_kw']
    df['new_batt_kw'] = df['new_adopters'] * df['batt_kw']
    df['new_batt_kwh'] = df['new_adopters'] * df['batt_kwh']

    # then add these values to values from last year to get cumulative values:
    df['number_of_adopters'] = df['number_of_adopters_last_year'] + df['new_adopters']
    df['market_value'] = df['market_value_last_year'] + df['new_market_value']

    df['pv_kw_cum'] = df['pv_kw_cum_last_year'] + df['new_pv_kw']
    df['batt_kw_cum'] = df['batt_kw_cum_last_year'] + df['new_batt_kw']
    df['batt_kwh_cum'] = df['batt_kwh_cum_last_year'] + df['new_batt_kwh']

    market_last_year = df[['agent_id', config.BA_COLUMN, 'tariff_id', 'sector_abbr', 'tech',
                            'market_share', 'max_market_share', 'number_of_adopters',
                            'market_value', 'initial_number_of_adopters', 'initial_pv_kw', 'initial_market_share', 'initial_market_value',
                            'pv_kw_cum', 'new_pv_kw', 'batt_kw_cum', 'batt_kwh_cum']]

    market_last_year.rename(columns={'market_share':'market_share_last_year', 
                               'max_market_share':'max_market_share_last_year',
                               'number_of_adopters':'number_of_adopters_last_year',
                               'market_value': 'market_value_last_year',
                               'pv_kw_cum':'pv_kw_cum_last_year',
                               'batt_kw_cum':'batt_kw_cum_last_year',
                               'batt_kwh_cum':'batt_kwh_cum_last_year'}, inplace=True)

    return df, market_last_year

def calc_diffusion_market_share(df, is_first_year):
    """
    Calculate the fraction of overall population that have adopted the technology in the current period.

    Parameters
    ----------
    df : pandas.DataFrame
        Attributes
        ----------
        df.payback_period : numpy.ndarray
            Payback period in years.
        df.max_market_share : numpy.ndarray
            Maximum market share as decimal percentage.
        df.current_market_share : numpy.ndarray
            Current market share as decimal percentage.
    is_first_year : bool
        If `True`, the new equivalent time (`teq2`) is equal to the original `teq_yr1` plus the increment defined in `teq`.
        Otherwise, `teq2` is equal to `teq` plus 2 years. 

    Returns
    -------
    numpy.ndarray
        The fraction of overall population that have adopted the technology
    
    Note
    ----
    1) This does not specify the actual new adoption fraction without knowing adoption in the previous period. 
    2) The relative economic attractiveness controls the p, q value in the Bass diffusion model.
    3) The current assumption is that only payback and MBS are being used, that pp is bounded [0-30] and MBS is bounded [0-120].

    """    
    df = calc_equiv_time(df); # find the 'equivalent time' on the newly scaled diffusion curve
    if is_first_year == True:
        df['teq2'] = df['teq'] + df['teq_yr1']
    else:
        df['teq2'] = df['teq'] + 2 # now step forward two years from the 'new location'
    
    df = bass_diffusion(df); # calculate the new diffusion by stepping forward 2 years

    df['bass_market_share'] = df.max_market_share * df.new_adopt_fraction # new market adoption    
    df['diffusion_market_share'] = np.where(df.market_share_last_year > df.bass_market_share, df.market_share_last_year, df.bass_market_share)
    
    return df

def set_bass_param(df, bass_params, override_p_value, override_q_value, override_teq_yr1_value):
    """
    Set the `p` & `q` parameters which define the Bass diffusion curve.

    `p` is the coefficient of innovation, external influence or advertising effect.
    `q` is the coefficient of imitation, internal influence or word-of-mouth effect.

    `p` & `q` values defined by :func:`diffusion_functions.bass_diffusion` can be overrode.

    Parameters
    ----------
    df : pandas.DataFrame
        Attributes
        ----------
        df.scaled_metric_value : numpy.ndarray
            Scaled value of economic attractiveness (range of 0 - 1)
    
    Returns
    -------
    pandas.DataFrame
        Input dataframe with `p` and `q` columns addded. 

    """
    # set p and q values
    df = pd.merge(df, bass_params, how = 'left', on  = [config.BA_COLUMN,'sector_abbr', 'tech'])
    
    # if override values were provided for p, q, or teq_yr1, apply them to all agents
    if override_p_value is not None:
        df.loc[:, 'p'] = override_p_value
    if override_q_value is not None:
        df.loc[:, 'q'] = override_q_value
    if override_teq_yr1_value is not None:
        df.loc[:, 'teq_yr1'] = override_teq_yr1_value
    return df
    

def bass_diffusion(df):
    """
    Calculate the fraction of population that diffuse into the max_market_share.

    Parameters
    ----------
    df : pandas.DataFrame
        Attributes
        ----------
        df.p : numpy.ndarray
            Bass diffusion parameter defining the coeffieicent of innovation.
        df.q : numpy.ndarray
            Bass diffusion parameter definint the coefficient of imitation.
        df.t : numpy.ndarray
            Number of years since the diffusion model began.
    
    Returns
    -------
    DataFrame
        Input dataframe with `new_adopt_fraction` column added. `new_adopt_fraction` represents the proportion of the overall population that will adopt the technology.

    Note
    ----
    This is different than the fraction of population that will adopt, which is the max market share.
    """
    df['f'] = np.e**(-1*(df['p'] + df['q']) * df['teq2'])
    df['new_adopt_fraction'] = (1-df['f']) / (1 + (df['q']/df['p'])*df['f']) # Bass Diffusion - cumulative adoption
    return df
    
def calc_equiv_time(df):
    """
    Calculate the "equivalent time" on the diffusion curve. This defines the gradient of adoption.

    Parameters
    ----------
    df : pandas.DataFrame
        Attributes
        ----------
            df.msly : numpy.ndarray
                Market share last year [at end of the previous solve] as decimal
            df.mms : numpy.ndarray
                Maximum market share as a decimal percentage.
            df.p : numpy.ndarray
                Bass diffusion parameter defining the coefficient of innovation.
            df.q : numpy.ndarray
                Bass diffusion paramter defining the coefficient of imitation.
        
    Returns
    -------
    pandas.DataFrame
        Input dataframe with `teq` column added. `teq` is the equivalent number of years after diffusion started on the diffusion curve.

    """
    df['mms_fix_zeros'] = np.where(df['max_market_share'] == 0, 1e-9, df['max_market_share'])
    df['ratio'] = np.where(df['market_share_last_year'] > df['mms_fix_zeros'], 0, df['market_share_last_year']/df['mms_fix_zeros'])
   #ratio=msly/mms;  # ratio of adoption at present to adoption at terminal period
    df['teq'] = np.log((1 - df['ratio']) / (1 + df['ratio']*(df['q']/df['p']))) / (-1*(df['p']+df['q'])) # solve for equivalent time
    return df


