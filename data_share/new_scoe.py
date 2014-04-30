# -*- coding: utf-8 -*-
"""
New scoe function
"""

# Add 'utility_type' as column to main df during data step. The only types of utilities allowed are [IOU, Muni, Coop] 

net_meter_avail = pd.read_csv('../data_share/NetMeterAvail.csv') 

# Before the turbine size is collected, left join net_meter_avail on df by ['state','sector', 'utility_type']
# each customer bin should now have a nem_system_limit_kw field, this represents the maximum capacity that is eligible for 
# net energy metering (nem)


# If turbine_size_kw > system_limit_kw, then you always want to size the turbine 
#s.t. energy production < 50% of load * the oversize_factor
    


''' Test codes
ic = 4000
fom = 20
vom = 0.01
naep = 1500

ann_elec_cons = 80000
nem_system_limit_kw = .000250

cap_list = [2.5,5,10,20,50,100,250,500,750,1000,1500,3000]


out = [scoe(ic, fom, vom, naep, cap, ann_elec_cons, nem_system_limit_kw, oversize_factor = 1.15, undersize_factor = 0.5) for cap in cap_list]
d = dict(zip(cap_list, out))
print 'optimal turbine is %0.1f kW' %(min(d, key = d.get))
'''

# new input here is 'nem_system_limit_kw'
def scoe(ic, fom, vom, naep, cap, ann_elec_cons, nem_system_limit_kw, oversize_factor = 1.15, undersize_factor = 0.5):
    
    if nem_system_limit_kw > cap:
        nem_factor = 1  
    else:
        nem_factor = undersize_factor
        
    if naep == 0:
        return float('inf')
    else:
        scoe = (ic + 30 * fom + 30 * naep * vom) / (30 * naep) # $/kWh
        # add in a penalty for oversizing that scales with the degree of oversizing
        oversized = ((naep * cap / ann_elec_cons) > (nem_factor * oversize_factor)) * ((naep * cap / ann_elec_cons) / (nem_factor * oversize_factor))
        undersized = ((naep * cap / ann_elec_cons) < (nem_factor * undersize_factor)) / ((naep * cap / ann_elec_cons) / (nem_factor * undersize_factor))
        scoe = scoe + oversized * 10 + undersized * 0.1 # Penalize under/over sizing    
         
        return scoe