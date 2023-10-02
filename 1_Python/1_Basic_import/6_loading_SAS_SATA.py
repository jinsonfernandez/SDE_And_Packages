import pandas as pd
import sas7bdat as SAS7BDAT

with SAS7BDAT('file.sas7bdat') as file:
    df_sas = file.to_data_frame()

sata_df = pd.read_sata('file.dta')
