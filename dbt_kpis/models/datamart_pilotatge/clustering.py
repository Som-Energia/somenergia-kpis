import pandas as pd

model_df = ref('widetable_socies')
print(model_df)

result_df = model_df.resample(rule='M', on='data_alta')['es_baixa'].sum().to_frame()

write_to_model(result_df, mode="overwrite")