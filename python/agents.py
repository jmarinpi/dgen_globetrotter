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
        agents_df : 'pd.DataFrame'
            Pandas Dataframe containing agents and their attributes.
            Index = agent ids, columns = agent attributes

        Returns
        -------
        agent_df : 'pd.DataFrame'
            Agents DataFrame
        agent_ids : 'ndarray'
            Array of agent ids
        agent_attrs : 'ndarray'
            Array of agent attributes
        attrs_types : 'pd.Series'
            Array of dtypes for each attribute
        """
        self.dataframe = agents_df
        self.agent_ids = agents_df.index
        self.agent_attrs = agents_df.columns
        self.attrs_types = agents_df.dtypes

    def __len__(self):
        """
        Return number of agents
        """
        return len(self.agent_ids)

    def __repr__(self):
        """
        Print number of agents and attributes
        """
        return ('{a} contains {n} agents with {c} attributes'
                .format(a=self.__class__.__name__,
                        n=len(self),
                        c=len(self.agent_attrs)))

    @property
    def check_attr_types(self):
        """
        Check to see if attribute types have changed
        """
        types = self.dataframe.dtypes
        check = self.attrs_types == types

        if not all(check):
            print('Attribute dtypes have changed')

    @property
    def update_agent_attrs(self):
        """
        Update agent class attributes
        """
        self.agent_ids = self.dataframe.index
        self.agent_attrs = self.dataframe.columns
        self.attrs_types = self.dataframe.dtypes

    def __add__(self, df):
        """
        Add agents to agents
        Parameters
        ----------
        df : 'pd.DataFrame'
            Pandas Dataframe containing agents to be added

        Returns
        -------
        agent_df : 'pd.DataFrame'
            Updated Agents DataFrame
        agent_ids : 'ndarray'
            Updated array of agent ids
        """
        # df_attrs = df.columns
        # Could just append, this would add attribute columns ...
        # mutual_attrs = df_attrs[np.in1d(df_attrs, self.agent_attrs)]
        # self.dataframe = self.dataframe.append(df[mutual_attrs])
        self.dataframe = self.dataframe.append(df)

        self.update_agent_attrs

    def add_attributes(self, df, on=None):
        """
        Add attributes to agents
        Parameters
        ----------
        df : 'pd.DataFrame'
            Pandas Dataframe containing new attributes for agents
        on : 'object'
            Pandas on kwarg, if None join on index

        Returns
        -------
        agent_df : 'pd.DataFrame'
            Updated Agents DataFrame
        attrs_types : 'pd.Series'
            Updated attribute types
        """
        if on is None:
            self.dataframe = self.dataframe.join(df, how='left')
        else:
            self.dataframe = pd.merge(self.dataframe, df, how='left', on=on)
        self.update_agent_attrs

    def compute_by_row(self, func, cores=None, in_place=True, **kwargs):
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
            If true, set self.dataframe = results of compute
            else return results of compute
        **kwargs
            Any additional kwargs for func

        Returns
        -------
        results_df : 'pd.Dataframe'
            Dataframe of agents after application of func
        """
        if cores is None:
            apply_func = partial(func, **kwargs)
            results_df = self.dataframe.apply(apply_func, axis=1)
        else:
            if 'ix' not in os.name:
                EXECUTOR = cf.ThreadPoolExecutor
            else:
                EXECUTOR = cf.ProcessPoolExecutor

            futures = []
            with EXECUTOR(max_workers=cores) as executor:
                for _, row in self.dataframe.iterrows():
                    futures.append(executor.submit(func, row, **kwargs))

                results = [future.result() for future in futures]
            results_df = pd.concat(results, axis=1).T

        if in_place:
            self.dataframe = results_df
            self.update_agent_attrs
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

        self.dataframe.to_pickle(file_name)


class Solar_Agents(Agents):
    """
    Solar Agents class instance
    """
    def __init__(self, agents_df, scenario_df):
        """
        Initialize Solar Agents Class
        Parameters
        ----------
        agents_df : 'pd.DataFrame'
            Pandas Dataframe containing agents and their attributes.
            Index = agent ids, columns = agent attributes
        scenario_df : 'pd.Dataframe'
            Pandas Dataframe containing scenario/solar specific attributes

        Returns
        -------
        agent_df : 'pd.DataFrame'
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
        self.add_attributes(scenario_df)

        self.update_agent_attrs
