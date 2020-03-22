from batcher import batches
import pandas as pd
import itertools


class PriorityMaker:

    def __init__(self, batches: list, selected_data):
        self.batches = batches
        self.input_data = selected_data
        self.cutoff_per_country = []
        self.priority_df = []
        self.box_priorities = []

    def make_priority(self):

        self.get_cutoff_times()
        self.create_priority_df()
        self.get_box_priorities()

        batches_df = pd.DataFrame(index=list(range(len(self.batches))))
        batches_df['BOX_NBRS'] = pd.Series(data=[list(itertools.chain(batch.keys())) for batch in self.batches])
        batches_df['PRIORITIES'] = batches_df['BOX_NBRS'].apply(
            lambda x: [self.box_priorities.at[i, 'PRIORITY'] for i in x])
        batches_df['HIGHEST_PRIORITY'] = batches_df['PRIORITIES'].apply(lambda x: min(x))
        batches_df['LOWEST_PRIORITY'] = batches_df['PRIORITIES'].apply(lambda x: max(x))

        return batches_df

    #  -----------------------------------------------------------------------------------------------------------------
    def get_cutoff_times(self):
        cutoff_per_country = pd.read_excel('Data/cut-off_per_country.xlsx')
        cutoff_per_country.dropna(inplace=True)
        cutoff_per_country.drop_duplicates(inplace=True)
        cutoff_per_country.rename(columns={'COUNTRY_CODE': 'SHIPTO_CNTRY'}, inplace=True)
        self.cutoff_per_country = cutoff_per_country

    def create_priority_df(self):
        priority_df = pd.DataFrame(self.cutoff_per_country['SHIPPING_CUTOFF'].drop_duplicates())
        priority_df = priority_df.sort_values('SHIPPING_CUTOFF')
        priority_df['PRIORITY'] = [i for i in range(len(priority_df))]
        self.priority_df = priority_df

    def get_box_priorities(self):
        self.box_priorities = self.input_data[['CNTR_NBR','SHIPTO_CNTRY','SHIP_VIA']].drop_duplicates()
        self.box_priorities = self.box_priorities.merge(
            self.cutoff_per_country[['SHIPTO_CNTRY','SHIP_VIA','SHIPPING_CUTOFF']]).merge(
            self.priority_df).set_index('CNTR_NBR')


DATA = pd.read_excel('Data/ups_pick_data_may_june_preprocessed.xlsx')
selected_data = DATA[DATA['DAY_DATE']=='2019-05-01 00:00:00'][['WAVE_NBR', 'CNTR_NBR','SHIPTO_CNTRY', 'SHIP_VIA', 'LOCN_BRCD', 'Aisle', 'Bay']].copy()
selected_data['nodelist'] = tuple(zip(selected_data['Aisle'], selected_data['Bay']))

priorities = PriorityMaker(batches, selected_data)
x = priorities.make_priority()





