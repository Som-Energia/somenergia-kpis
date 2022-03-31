# -*- coding: utf-8 -*-
"""
Created on Tue Jan 25 13:06:18 2022

@author: somenergia
"""

import pandas as pd
import numpy as np
import os

# =============================================================================
# Carrega i prepara dades estàtiques:
#     - calendari OMIE
# =============================================================================
#input
#   - fpathname al calendari
#output
#   - df_prevEcalomie([dia, dia_pagos'])
fpath = 'N:\\80_Scripts\\01_solvencia\\'
fnamecal = 'Calendari_OMIE.xlsx'

df_cal = pd.read_excel(fpath+fnamecal)
df_cal['dia_pagos'] = df_cal['Fecha Pagos'].map(lambda x: pd.to_datetime(x.date()))
df_cal['data'] = df_cal['Día'].map(lambda x: pd.to_datetime(x.date()))
df_cal = df_cal[['data', 'dia_pagos']]

# =============================================================================
# Carrega i prepara històric compra OMIE (volums i preus)
# =============================================================================
#input
# - fpathname carpeta marginalpdbc
# - fpathname carpeta pdbc
#output
#   df_prevE_historic ('dia', 'cons_MWh', 'preu_EurMWh', 'import_real_Eur)

fpath_omie_pdbc = 'N:\\30_OMIE\\preentrada\\PDBC\\'
fpath_omie_marginalpdbc = 'N:\\30_OMIE\\preentrada\\MARGINALPDBC\\'

listpdbc = os.listdir(fpath_omie_pdbc)
pdbccolumns = ['Any', 'mes', 'dia', 'periode', 'Energia_comprada_MWh']

df_pdbc_end = pd.DataFrame(columns = pdbccolumns)
for fname in listpdbc:
    df_pdbc = pd.read_csv(fpath_omie_pdbc+fname, sep=';', skiprows=[0], header=None, comment='*')
    df_pdbc = df_pdbc[[0,1,2,3,5]]
    df_pdbc.columns = pdbccolumns
    df_pdbc_end = pd.concat([df_pdbc_end, df_pdbc], axis=0, ignore_index=True)

listmargpdbc = os.listdir(fpath_omie_marginalpdbc)
marginalpdbccolumns = ['Any', 'mes', 'dia', 'periode', 'Preu_Eur_MWh']
df_mpdbc_end = pd.DataFrame(columns = marginalpdbccolumns)
for fname in listmargpdbc:
    df_mpdbc = pd.read_csv(fpath_omie_marginalpdbc+fname, sep=';', skiprows=[0], header=None, comment='*')
    df_mpdbc = df_mpdbc[[0,1,2,3,5]]
    df_mpdbc.columns = marginalpdbccolumns
    df_mpdbc_end = pd.concat([df_mpdbc_end, df_mpdbc], axis=0, ignore_index=True)


df_hist = pd.merge(df_pdbc_end, df_mpdbc_end, how='left', on=['Any', 'mes', 'dia', 'periode'])
df_hist['Energia_comprada_MWh'] = df_hist['Energia_comprada_MWh']*-1
df_hist['Import_Energia_Eur'] = df_hist['Energia_comprada_MWh']*df_hist[ 'Preu_Eur_MWh']
df_hist['Import_EnergiaiIVA_Eur'] = df_hist['Import_Energia_Eur']*1.21
df_hist = df_hist.sort_values(['Any', 'mes', 'dia', 'periode'])

# =============================================================================
# Carrega inputs per a previsió evolució pagaments i garanties OMIE
#   - consum
#   - preus
# =============================================================================
#input
#   - fpathname xlsx previsió consum
#   - fpathname xlsx previsió preus
#output
#   - df_prevE_previsio('dia', 'prev_MWh', 'preu_EurMWh', 'import_prev_Eur) 

fpath = 'N:\\80_Scripts\\01_solvencia\\'
fname_prevE = 'prevision-neuro_SOMEC01_20220126.xlsx'

df_prev = pd.read_excel(fpath+fname_prevE, skiprows=[0,1])
UP = str(pd.read_excel(fpath+fname_prevE, nrows=1, usecols=0).columns[0])[4:]
colname = UP+'_MWh_bc'
df_prev = df_prev[['Fecha', 'Hora', 'Base']]
df_prev.columns = ['Data', 'Periode', colname]

##possibilitat d'entrar un excel similar al previsió consum però amb previsió preu
## de moment marquem preu únic per a totes les hores:
df_prev['Previsio_Preu'] = 224.24
df_prev['Import_Energia_Eur'] = df_prev[colname]*df_prev['Previsio_Preu']
df_prev['Import_EnergiaiIVA_Eur'] = df_prev['Import_Energia_Eur']*1.21



# =============================================================================
# Unifica en una taula volums comprats, previsió consum i preus
# =============================================================================
#input
#   - df_prevE_historic(['dia', 'import_diari_Eur']) (o fer groupby dia)
#   - df_prevE_previsio(['dia', 'import_diari_Eur']) (o fer groupby dia)
#output
#   - df_prevE_unificat(['dia', 'import_diari_Eur'])


#fem groupby per dia:
df_prev_diari = df_prev.groupby(['Data']).sum()[[colname, 'Import_EnergiaiIVA_Eur']]
df_prev_diari['data'] = pd.to_datetime(df_prev_diari.index.copy())
df_prev_diari.columns = ['Energia_MWh', 'Import_EnergiaiIVA_Eur', 'data']
df_prev_diari['tipus'] = 'Previst'

df_hist['Data'] = df_hist['Any'].map(str) + '-'+df_hist['mes'].map(str) + '-'+df_hist['dia'].map(str)
df_hist_diari = df_hist.groupby(['Data']).sum()[['Energia_comprada_MWh','Import_EnergiaiIVA_Eur']]
df_hist_diari['data'] = pd.to_datetime(df_hist_diari.index.copy())
df_hist_diari.columns = ['Energia_MWh', 'Import_EnergiaiIVA_Eur', 'data']
df_hist_diari['tipus'] = 'Comprat'
df_hist_diari = df_hist_diari.sort_values(['data'])
df_hist_diari.tail(1).index

df_prev_diari['in_historic'] = np.where(df_prev_diari['data'].isin(df_hist_diari['data']), True, False)
df_prev_diari = df_prev_diari[df_prev_diari['in_historic']==False].copy()
df_prev_diari.drop(['in_historic'], axis=1, inplace=True)

df_unif = pd.merge(df_hist_diari, df_prev_diari,  how='outer')


# =============================================================================
# carrega columna dia pagos, simula pagos i evolució garanties (check si es correcte)
# =============================================================================
#input
#   - df_prevE_unificat([dia, import_diari_Eur])
#   - df_prevEcalomie([dia, dia_pagos'])
#output
#   - df_prevE_simulacio([dia, import_pago_Eur, import_garantia_exigida_Eur])


df_sim = df_unif.merge(df_cal, how='left', on=['data'])# left_index=True, right_index=True)

# fes un groupby de import per dia de pagos -> pagament que s'ha de fer aquell dia
df_aux = df_sim.groupby('dia_pagos', as_index=False).sum()
df_aux = df_aux[['dia_pagos',  'Import_EnergiaiIVA_Eur']]
df_aux.columns = ['data', 'pagament_OMIE']
import_garantia_exigida_actual = 0 #1600000
df_aux.loc[0, 'pagament_OMIE'] = df_aux.loc[0, 'pagament_OMIE'] + import_garantia_exigida_actual


# el groupby anterior posa una columna que sigui pagament
df_sim = df_sim.merge(df_aux, how='left', on=['data']).copy()
df_sim['pagament_OMIE'] = df_sim['pagament_OMIE'].replace('nan', np.nan).fillna(0)
# càlcul de la garantia
df_sim['garanties_pago'] = 0
df_sim['garanties_operar'] = 0
offer_margin = 1.75 #marge addicional que es fa per a que entri la oferta del dia següent

for index, row in df_sim.iterrows():
   if index==0:
      df_sim.loc[index, 'garanties_pago'] = df_sim.loc[index, 'Import_EnergiaiIVA_Eur']+ import_garantia_exigida_actual
      df_sim.loc[index, 'garanties_operar'] = offer_margin*df_sim.loc[index+1, 'Import_EnergiaiIVA_Eur']
   elif index<len(df_sim)-1:
      df_sim.loc[index,'garanties_pago'] = df_sim.loc[index, 'Import_EnergiaiIVA_Eur']+ df_sim.loc[index-1, 'garanties_pago']-df_sim.loc[index-1, 'pagament_OMIE'] 
      df_sim.loc[index, 'garanties_operar'] = offer_margin*df_sim.loc[index+1, 'Import_EnergiaiIVA_Eur']
   else:
      df_sim.loc[index,'garanties_pago'] = df_sim.loc[index, 'Import_EnergiaiIVA_Eur']+ df_sim.loc[index-1, 'garanties_pago']-df_sim.loc[index-1, 'pagament_OMIE']
      df_sim.loc[index, 'garanties_operar'] = offer_margin*df_sim.loc[index, 'Import_EnergiaiIVA_Eur'] #assumim mateixa energia comprada l'endemà al final de la sèrie.

df_sim['garanties_totals'] = df_sim['garanties_pago']+df_sim['garanties_operar'] 

#df_sim.to_csv(fpath+'test202201261133.csv', sep=';')
# =============================================================================
# simulador per a la taula volum preus
# =============================================================================
#google calc
