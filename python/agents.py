# -*- coding: utf-8 -*-
"""
Module to intialize agents class along with methods to apply functions to the dataframe on row or frame. 
"""

import concurrent.futures as cf
from functools import partial
import os
import pandas as pd


class Agents(object):
    """
    Agents class instance
    """
    def __init__(self, agents_df):
        """
        Initialize Agents Class
        
        Parameters
        ----------
        agents_df : pandas.DataFrame
            Pandas Dataframe containing agents and their attributes.
            The index should be the agent ids, and columns should be individual agent attributes.

        Returns
        -------
        agent_df : pandas.DataFrame
            Agents DataFrame
        agent_ids : numpy.ndarray
            Array of agent ids
        agent_attrs : numpy.ndarray
            Array of agent attributes
        attrs_types : pandas.Series
            Array of dtypes for each attribute

        Attributes
        ----------
        df : pandas.DataFrame
            The agents_df object
        ids : pandas.Index
            The index of the agents_df object
        attrs : pandas.DataFrame.columns
            the columns of the agents_df object
        types : data-types
            the datatypes of the agents_df columns

        """
        self.df = agents_df
        self.ids = agents_df.index
        self.attrs = agents_df.columns
        self.types = agents_df.dtypes

    def __len__(self):
        """
        Return number of agents
        """
        return len(self.ids)

    def __repr__(self):
        """
        Print number of agents and attributes
        """
        return ('{a} contains {n} agents with {c} attributes'
                .format(a=self.__class__.__name__,
                        n=len(self),
                        c=len(self.attrs)))

    @property
    def check_types(self):
        """
        Check to see if attribute types have changed
        """
        types = self.df.dtypes
        check = self.types == types

        if not all(check):
            print('Attribute dtypes have changed')

    @property
    def update_attrs(self):
        """
        Update agent class attributes
        """
        self.ids = self.df.index
        self.attrs = self.df.columns
        self.types = self.df.dtypes

    def __add__(self, df):
        """
        Add agents to agents

        Parameters
        ----------
        df : pandas.DataFrame
            Pandas Dataframe containing agents to be added

        Returns
        -------
        agent_df : pandas.DataFrame
            Updated Agents DataFrame
        agent_ids : numpy.ndarray
            Updated array of agent ids
        """
        # df_attrs = df.columns
        # Could just append, this would add attribute columns ...
        # mutual_attrs = df_attrs[np.in1d(df_attrs, self.attrs)]
        # self.df = self.df.append(df[mutual_attrs])
        self.df = self.df.append(df)

        self.update_attrs

    def add_attrs(self, attr_df, on=None):
        """
        Add attributes to agents

        Parameters
        ----------
        df : pandas.DataFrame
            Pandas Dataframe containing new attributes for agents
        on : str or None
            Pandas `on` kwarg, if None join on index

        Returns
        -------
        agent_df : pandas.DataFrame
            Updated Agents DataFrame
        attrs_types : pandas.Series
            Updated attribute types
        """
        if on is None:
            self.df = self.df.join(attr_df, how='left')
        else:
            self.df = self.df.reset_index()
            self.df = pd.merge(self.df, attr_df, how='left', on=on)
            self.df = self.df.set_index('agent_id')
        self.update_attrs

    def on_row(self, func, cores=None, in_place=True, **kwargs):
        """
        Apply function to agents on an agent by agent basis. Function should
        return a df to be merged onto the original df. Can utilize multiple cores when
        a function is embarrassingly parallel. Number of cores is defined by :py:const:`config.LOCAL_CORES`.

        Parameters
        ----------
        func : function
            Function to be applied to each agent
            Must take a pd.Series as the arguement
        cores : int
            Number of cores to use for computation
        in_place : bool
            If true, set self.df = results of compute
            else return results of compute
        **kwargs
            Any kwargs for func

        Returns
        -------
        results_df : pandas.DataFrame
            Dataframe of agents after application of func
        """
#        self.df.reset_index(inplace=True)

        if cores is None:
            apply_func = partial(func, **kwargs)
            results_df = self.df.apply(apply_func, axis=1)
        else:
            if 'ix' not in os.name:
                EXECUTOR = cf.ThreadPoolExecutor
            else:
                EXECUTOR = cf.ProcessPoolExecutor

            futures = []
            with EXECUTOR(max_workers=cores) as executor:
                for _, row in self.df.iterrows():
                    futures.append(executor.submit(func, row, **kwargs))

                results = [future.result() for future in futures]
            results_df = pd.concat(results, axis=1).T

            results_df.index.name = 'agent_id'
            

        if in_place:
            self.df = results_df
#            self.df = pd.merge(self.df, results_df, on='agent_id')
#            self.df.set_index('agent_id', inplace=True)
            self.update_attrs
        else:
            return results_df

    def on_frame(self, func, func_args=None, in_place=True, **kwargs):
        """
        Apply function to agents using agent.df

        Parameters
        ----------
        func : function
            Function to be applied to agent.df
            Must take a `pandas.DataFrame` as the arguement
        func_args : list of str or None
            args for func
        in_place : bool
            If true, set `self.df` to the results of compute,
            else return results of compute
        **kwargs
            Any kwargs for func

        Returns
        -------
        results_df : pandas.DataFrame
            Dataframe of agents after application of func
        """
        if func_args is None:
            results_df = func(self.df, **kwargs)
        elif isinstance(func_args, list):
            results_df = func(self.df, *func_args, **kwargs)
        else:
            results_df = func(self.df, func_args, **kwargs)

        if in_place:
            self.df = results_df
            self.update_attrs
        else:
            return results_df

    def to_pickle(self, file_name):
        """
        Save agents to pickle file

        Parameters
        ----------
        file_name : str
            File name for agents pickle file
        """
        if not file_name.endswith('.pkl'):
            file_name = file_name + '.pkl'

        self.df.to_pickle(file_name)


class Solar_Agents(Agents):
    """
    Solar Agents class instance
    """
    def __init__(self, agents_df, scenario_df):
        """
        Initialize Solar Agents Class

        Parameters
        ----------
        agents_df : pandas.DataFrame
            Pandas Dataframe containing agents and their attributes.
            The index should be the agent ids, and columns should be individual agent attributes.
        scenario_df : pandas.DataFrame
            Pandas Dataframe containing scenario/solar specific attributes

        Returns
        -------
        agent_df : pandas.DataFrame
            Agents DataFrame
        agent_ids : numpy.ndarray
            Array of agent ids
        agent_attrs : numpy.ndarray
            Array of agent attributes
        attrs_types : pd.Series
            Array of dtypes for each attribute
        """
        Agents.__init__(self, agents_df)
        # Filter out attributes not needed for solar?
        self.add_attrs(scenario_df)

        self.update_attrs
