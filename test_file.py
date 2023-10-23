from get_dataframes import get_data
from datetime import datetime

pricing_types_df, products_reference_df, reprices_errors_log_df, reprices_log_df, calendar_df = get_data()


print(reprices_log_df.loc[reprices_log_df['date_reprice'] == datetime(2023,9,20)])

print(reprices_log_df.iloc[0,'product_id'])