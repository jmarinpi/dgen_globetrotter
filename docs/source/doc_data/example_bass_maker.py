#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 14:37:52 2019

@author: skoebric
"""


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def dfsn_abm(p, q, M, T):
    """Convenience function to generate a stochastic bass curve for demonstration purposes"""
    adopt = list()
    x = np.zeros((M,), np.float32)
    x_temp = np.zeros((M,), np.float32)
    for t in range(T):
        for i in range(1,M):
            prob = (p + q * (sum(x) / M)) * (1 - x[i])
            if np.random.uniform(0,1,1) <= prob:
                x_temp[i] = 1
        x = x_temp
        adopt.append(sum(x))
    return adopt

sim_dfsn = [dfsn_abm(0.01, 0.3, 1000, 25) for _ in range(20)]



#%%
import seaborn as sns



df = pd.DataFrame(np.array(sim_dfsn))
df_trend = df.melt(var_name = 'timestep')

start_mean = df_trend.loc[df_trend['timestep'] == 0]['value'].mean()

df_diff = df.T.diff().fillna(start_mean).T.melt(var_name = 'timestep')

fig, ax = plt.subplots()
sns.lineplot(x = 'timestep', y = 'value', data=df_trend, ax = ax, label = 'Total Adopters')
sns.lineplot(x = 'timestep', y = 'value', data = df_diff, ax = ax, label = 'New Adopters')

ax.annotate('$\it{p}$ is leading\nearly adoption',
            xy = (4.7, 125),
            xytext = (5, 100),
            textcoords = 'offset points',
            arrowprops = {'arrowstyle':'simple', 'color':'k'},
            fontsize = 10).draggable()

ax.annotate('$\it{q}$ kicks in,\nadoption grows',
            xy = (9.2, 83),
            xytext = (5, 100),
            textcoords = 'offset points',
            arrowprops = {'arrowstyle':'simple', 'color':'k'},
            fontsize = 10).draggable()

ax.annotate('A limit\nis reached.',
            xy = (20.4, 972),
            xytext = (0, -200),
            textcoords = 'offset points',
            arrowprops = {'arrowstyle':'simple', 'color':'k'},
            fontsize = 10).draggable()

ax.annotate('$\it{p}$ = 0.01  $\it{q}$ = 0.3',
            xy = (20, 900),
            fontsize = 10).draggable()

ax.set_xlabel('Time Increment')
ax.set_ylabel('Number of Adopters')
ax.set_title('Example Bass Diffusion Curve')