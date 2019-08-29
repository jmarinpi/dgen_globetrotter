#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 27 17:17:37 2019

@author: skoebric
"""

#%%
#VISUALIZATIONS

def subsidy_calculator(row): #not used right now, calculates the total electricity bill for each agent based on subsidly levels
   monthly_con = row['load_per_customer_in_bin_kwh'] / 12 #exceeding CS by 50%, and you don't get CS!!!
   if 'estrato' in row['tariff_class']:
       estrato = int(row['tariff_class'].split('_')[1])
       if estrato < 4:
           if row['elevation'] > 1000:
               sub_limit = 173
           elif row['elevation'] < 1001:
               sub_limit = 130
       else:
           sub_limit = 0

       if monthly_con > (sub_limit * 1.5):
           sub_limit = 0

       excess_con = max(0, monthly_con - sub_limit)

       if estrato == 1:
           sub_rate = 0.45
       elif estrato == 2:
           sub_rate = 0.55
       elif estrato == 3:
           sub_rate = 0.85
       elif estrato == 4:
           sub_rate = 1
       elif estrato > 4:
           sub_rate = 1.2

       if estrato < 4:
           if excess_con == 0:
               bill = monthly_con * sub_rate * row['unsub_cost']
           elif excess_con > 0:
               bill = (sub_limit * sub_rate * row['unsub_cost']) + (excess_con * row['unsub_cost'])
       elif estrato > 3:
           bill = monthly_con * sub_rate * row['unsub_cost']

       bill = bill * 12 #back to annual basis

       return bill

   else:
       return row['unsub_cost'] * row['load_per_customer_in_bin_kwh'] #don't do anyting for C&I, gov

subsidy_viz_df = df.copy()[['load_per_customer_in_bin_kwh', 'tariff_class','elevation','unsub_cost']]
subsidy_viz_df['average_customer_annual_bill'] = subsidy_viz_df.swifter.apply(subsidy_calculator, axis = 1)
subsidy_viz_df = subsidy_viz_df.loc[subsidy_viz_df['load_per_customer_in_bin_kwh'] < 20000]

import seaborn as sns

sns.scatterplot('load_per_customer_in_bin_kwh', 'average_customer_annual_bill', hue='tariff_class', alpha=0.2, data = subsidy_viz_df)

#%%
