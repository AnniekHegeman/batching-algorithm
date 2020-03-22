import pandas as pd
import numpy as np



class PreProcessor:

    def __init__(self, filename: str, aisles: list):
        self.filename = filename
        self.data = pd.read_excel(filename+'.xlsx')
        self.aisles = aisles
        self.aisles_df = 0

    def run_preprocessing(self):
        try:
            pd.read_excel(self.filename+'_preprocessed.xlsx')
        except:
            self.reset_columns()  # select and add necessary columns on input DataFrame
            self.rename_columns()  # rename columns
            self.determine_aisles()  # create aisles column and fill with subset of LOCN_BRCD column
            self.determine_pick_type()  # create and fill 'pick_type' column
            self.filter_data()  # filter data on pick-type and business division
            self.create_levels()  # create levels column and possibly filter on level
            self.data.sort_values('PICK_BEGIN_DATE', inplace=True)
            self.create_time_columns()  # create time columns, account for time difference and create day timestamp
            self.data.dropna(inplace=True)
            self.data['CARTON_SIZE'] = self.data['CARTON_SIZE'].apply(lambda x: self.change_b_to_t(x))
            self.day_of_week_filter()  # remove weekends from data
            self.create_aisles_df()
            self.generate_nodes()
            self.data.drop(columns=['DIV', 'Pick type', 'STD_PACK_QTY', 'STD_CASE_QTY',], axis=1, inplace=True)
            self.relevant_ship_via()
            self.data = self.data.sort_values('PKT_WAVE_DATE_TIME')
            self.data.to_excel(self.filename+'_preprocessed.xlsx', index=False)

    def reset_columns(self):
        self.data = self.data[['DIV', 'CNTR_NBR', 'PKT_WAVE_DATE_TIME', 'PICK_BEGIN_DATE', 'PICK_END_DATE',
                               'PKT_CTRL_NBR', 'SHIPTO_CNTRY', 'SHIP_VIA', 'WAVE_NBR', 'SKU_BRCD', 'STD_PACK_QTY',
                               'STD_CASE_QTY', 'NBR_UNITS_PICKED', 'REF_FIELD_1', 'FROM_LOCN_BRCD', 'CARTON_TYPE',
                               'CARTON_SIZE', 'USER_ID']].copy()

    def rename_columns(self):
        self.data.rename(columns={'FROM_LOCN_BRCD': 'LOCN_BRCD'}, inplace=True)

    def determine_aisles(self):
        self.data['Aisle'] = self.data['LOCN_BRCD'].apply(lambda x: x[2:4])

    def determine_pick_type(self):
        self.data['Pick type'] = ''
        for idx in self.data.index:
            if self.data.at[idx, 'NBR_UNITS_PICKED'] % self.data.at[idx, 'STD_CASE_QTY'] == 0:
                self.data.at[idx, 'Pick type'] = 'Pallet'
            elif self.data.at[idx, 'NBR_UNITS_PICKED'] % self.data.at[idx, 'STD_PACK_QTY'] == 0:
                self.data.at[idx, 'Pick type'] = 'Shipper'
            else:
                self.data.at[idx, 'Pick type'] = 'Unit'
    
    def filter_data(self):
        self.data = self.data[self.data['DIV'] == 1]
        self.data = self.data[self.data['Pick type'].isin(['Unit', 'Shipper'])]
        self.data = self.data[self.data['Aisle'].isin(aisles)]
    
    def create_levels(self):
        self.data['level'] = self.data['LOCN_BRCD'].apply(lambda x: str(x[6:8]))
        # self.data = self.data[self.data['level'] <= '10']

    def create_time_columns(self):
        time_columns = ['PKT_WAVE_DATE_TIME', 'PICK_BEGIN_DATE', 'PICK_END_DATE']
        for column in time_columns:
            self.data[column] = self.data[column] + pd.Timedelta('6 hours')
        self.data['DAY_DATE'] = pd.DatetimeIndex(self.data.PICK_BEGIN_DATE).normalize()

    def change_b_to_t(self, x):
        if x[0] == 'T':
            value = 'B' + x[1:]
        else: value = x
        return value

    def day_of_week_filter(self):
        self.data = self.data[self.data['DAY_DATE'].dt.dayofweek < 5]

    def create_aisles_df(self):
        self.aisles.sort(reverse=True)
        nodelist = [[x, x+1] for x in range(1, 1000, 2)]
        nodelist = nodelist[0:len(self.aisles)]
        self.aisles_df = pd.DataFrame(index=self.aisles, data=nodelist, columns=['odd', 'even'])

    def generate_nodes(self):
        self.data['Bay'] = self.data.LOCN_BRCD.str[4:6]
        self.data['Bay'] = self.data['Bay'].astype(int)
        self.data['Bay'] = self.data['Bay'].apply(lambda x: self.bays(x))
        self.data.dropna(inplace=True)
        self.data['Aisle'] = self.data[['Aisle','Bay']].values.tolist()
        self.data['Aisle'] = self.data['Aisle'].apply(lambda x: self.location_parser(x))

    def location_parser(self, x):
        if int(x[1]) % 2 == 0:
            aisle_nr = self.aisles_df.at[x[0], 'even']
        elif int(x[1]) % 2 != 0:
            aisle_nr = self.aisles_df.at[x[0], 'odd']
        return aisle_nr

    def bays(self, i):
        """ Transforms 'Bay' column to y values of the nodes. """
        lower_part = {Bay: int((Bay) / 2) for Bay in range(18)}
        upper_part = {Bay: int((Bay) / 2) + 1 for Bay in range(18, 36)}
        switcher = {**lower_part, **upper_part}
        return switcher.get(i, np.nan)

    def relevant_ship_via(self):
        relevant_values = ['ED67', 'ED68', 'ED69', 'ED70', 'ED74', 'ED95']
        self.data = self.data[self.data['SHIP_VIA'].isin(relevant_values)]


if __name__ == '__main__':
    aisles = ['AB','AC','AD','AE','AF','AG','AH','AJ','AK','AL','AM','AN','AP','AQ','AR','AS','AT','AU','AV','AW','AX']
    data_input = 'Data/Batching historical picks_2019 December'
    preprocessor = PreProcessor(data_input, aisles)
    preprocessor.run_preprocessing()
