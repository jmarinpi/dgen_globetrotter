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
        agents_df : 'pd.df'
            Pandas Dataframe containing agents and their attributes.
            Index = agent ids, columns = agent attributes

        Returns
        -------
        agent_df : 'pd.df'
            Agents DataFrame
        agent_ids : 'ndarray'
            Array of agent ids
        agent_attrs : 'ndarray'
            Array of agent attributes
        attrs_types : 'pd.Series'
            Array of dtypes for each attribute
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
        df : 'pd.df'
            Pandas Dataframe containing agents to be added

        Returns
        -------
        agent_df : 'pd.df'
            Updated Agents DataFrame
        agent_ids : 'ndarray'
            Updated array of agent ids
        """
        # df_attrs = df.columns
        # Could just append, this would add attribute columns ...
        # mutual_attrs = df_attrs[np.in1d(df_attrs, self.attrs)]
        # self.df = self.df.append(df[mutual_attrs])
        self.df = self.df.append(df)

        self.update_attrs

    def add_attrs(self, df, on=None):
        """
        Add attributes to agents
        Parameters
        ----------
        df : 'pd.df'
            Pandas Dataframe containing new attributes for agents
        on : 'object'
            Pandas on kwarg, if None join on index

        Returns
        -------
        agent_df : 'pd.df'
            Updated Agents DataFrame
        attrs_types : 'pd.Series'
            Updated attribute types
        """
        if on is None:
            self.df = self.df.join(df, how='left')
        else:
            self.df = pd.merge(self.df, df, how='left', on=on)
        self.update_attrs

    def on_row(self, func, cores=None, in_place=True, **kwargs):
        """
        Apply function to agents on an agent by agent basis
        Parameters
        ----------
        func : 'function'
            Function to be applied to each agent
            Must take a pd.Series as the arguement
        cores : 'int'
            Number of cores to use for computation
        in_place : 'bool'
            If true, set self.df = results of compute
            else return results of compute
        **kwargs
            Any kwargs for func

        Returns
        -------
        results_df : 'pd.df'
            Dataframe of agents after application of func
        """

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

        if in_place:
            self.df = results_df
            self.update_attrs
        else:
            return results_df

    def on_frame(self, func, func_args=None, in_place=True, **kwargs):
        """
        Apply function to agents using agent.df
        Parameters
        ----------
        func : 'function'
            Function to be applied to agent.df
            Must take a pd.df as the arguement
        func_args : 'object'
            args for func
        in_place : 'bool'
            If true, set self.df = results of compute
            else return results of compute
        **kwargs
            Any kwargs for func

        Returns
        -------
        results_df : 'pd.df'
            Dataframe of agents after application of func
        """
        if func_args is None:
            results_df = func(self.df, **kwargs)
        elif isinstance(func_args, list):
            results_df = func(self.df, *func_args, **kwargs)
        else:
            results_df = func(self.df,func_args, **kwargs)

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
        file_name : 'sting'
            File name for agents pickle file

        Returns
        -------

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
        agents_df : 'pd.df'
            Pandas Dataframe containing agents and their attributes.
            Index = agent ids, columns = agent attributes
        scenario_df : 'pd.df'
            Pandas Dataframe containing scenario/solar specific attributes

        Returns
        -------
        agent_df : 'pd.df'
            Agents DataFrame
        agent_ids : 'ndarray'
            Array of agent ids
        agent_attrs : 'ndarray'
            Array of agent attributes
        attrs_types : 'pd.Series'
            Array of dtypes for each attribute
        """
        Agents.__init__(self, agents_df)
        # Filter out attributes not needed for solar?
        self.add_attrs(scenario_df)

        self.update_attrs
