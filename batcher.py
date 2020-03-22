import pandas as pd
import numpy as np
import time
from sorter import SortNodesFactory
import itertools


# %%

class Batch:

    def __init__(self, raw_data: pd.DataFrame, dist_matrix: pd.DataFrame, capacity: int, refpos: list, cust_name: str,
                 date: str):
        self.raw_data = raw_data
        self.distance_matrix = dist_matrix
        self.capacity = capacity
        self.refpos = refpos
        self.date = date
        self.boxes = []
        self.boxes_to_batch = []
        self.savings_matrix = []
        self.sorter = SortNodesFactory.create(cust_name)
        self.batches = []

    # ------------------------------------------------------------------------------------------------------------------

    def batch(self):
        '''USED TO RUN THE BATCHING FUNCTION, PERFORMS CHECKS WHETHER BATCHING IS NEEDED OR NOT'''
        self.preprocesser()  # RUN PREPROCESSING FUNCTION
        # CHECK IF BATCHING IS NEEDED, IF NOT, ADD TO RESULTS, IF NEEDED, BATCH
        if len(self.boxes) <= self.capacity:
            batch = {x: self.boxes.at[x, 'nodelist'] for x in self.boxes.index}
            self.batches.append(batch)
        else:
            print("Start batching process...")  # progress
            self.run_batching()

    def run_batching(self):

        '''FUNCTION BATCHES ORDERS TOGETHER (BOXES IN UPS CASE)
        INPUT: DATAFRAME WITH ORDER_ID AND NODELIST; CAPACITY CONSTRAINT, DISTANCE MATRIX, REFFERENCE POSITION
        OUTPUT: LIST OF DICTIONARIES, EACH DICTIONARY IS A BATCH; keys=ORDER_ID, values=LOCATIONS'''

        NESTED_NODELIST = list(self.boxes['nodelist'])
        self.boxes['sorted_nodes'] = [self.refpos + self.sorter.sort(nodelist) + self.refpos for nodelist in
                                      NESTED_NODELIST]
        self.boxes['distance_per_box'] = self.boxes['sorted_nodes'].apply(lambda x: self.distance(x))
        self.boxes_to_batch = list(self.boxes.index)

        self.savings_matrix = self.generate_savings_matrix_ip()

        while len(self.boxes_to_batch) > 0:
            try:  # GENERATE SAVINGS_LIST(matrix) TO PICK INITIAL PAIR AND ADD INITIAL PAIR TO BATCH
                SAVINGS_LIST = self.savings_list_ip(self.savings_matrix)
                batch = self.create_initial_pair(SAVINGS_LIST)  # dtype=dict
            except:  # IF ONLY ONE PAIR LEFT IN BOXES TO BATCH: CREATE BATCH WITH ONLY ONE ITEM
                batch = {self.boxes_to_batch[0]: self.boxes.at[self.boxes_to_batch[0], 'nodelist']}

            while len(batch) < self.capacity and len(self.boxes_to_batch) > 0:
                print("Orders to batch: {}".format(len(self.boxes_to_batch)))  # PROGRESS DISPLAY
                savings_mat = self.savings_matrix_eb(batch)  # MATRIX['box', 'saving']
                order_to_add = self.add_to_batch(savings_mat)  # SELECT BOX AND REMOVE FROM BOXES TO BATCH
                batch[order_to_add] = self.boxes.at[order_to_add, 'nodelist']  # ADD NEW BOX TO BATCH DICTIONARY

            batch_orders = list(batch.keys())
            self.savings_matrix.drop(batch_orders, axis=0, inplace=True)  # REMOVE FROM SAVINGS_MATRIX
            self.savings_matrix.drop(batch_orders, axis=1, inplace=True)  # REMOVE FROM SAVINGS_MATRIX
            self.batches.append(batch)  # APPEND BATCH TO TOTAL LIST OF BATCHES

    # ------------------------------------------------------------------------------------------------------------------
    # HELPER FUNCTIONS
    # ------------------------------------------------------------------------------------------------------------------

    def preprocesser(self):
        selected_data = self.raw_data[self.raw_data['DAY_DATE'] == self.date][
            ['WAVE_NBR', 'CNTR_NBR', 'LOCN_BRCD', 'Aisle', 'Bay']].copy()
        selected_data['nodelist'] = tuple(zip(selected_data['Aisle'], selected_data['Bay']))
        self.boxes = pd.DataFrame(selected_data.groupby('CNTR_NBR')['nodelist'].apply(set).apply(list))

    def distance(self, nodelist):
        '''FUNCTION RETRIEVES DISTANCES FROM THE DISTANCE MATRIX'''
        distance = 0
        for i in range(len(nodelist) - 1):
            dist = self.distance_matrix.at[str(nodelist[i]), str(nodelist[i + 1])]
            if np.isnan(dist):
                dist = self.distance_matrix.get_value(nodelist[i + 1], nodelist[i])
            distance += dist
        return distance

    def generate_savings_matrix_ip(self):
        '''GENERATE SAVINGS MATRIX AND FILL WITH FILL SAVINGS MATRIX FUNCTION'''
        matrix = pd.DataFrame(data=[[(x, y) for x in self.boxes_to_batch] for y in self.boxes_to_batch],
                              index=self.boxes_to_batch, columns=self.boxes_to_batch).applymap(
            lambda x: sorted(x)).applymap(
            lambda x: [x[0], x[1], self.fill_savings_matrix_ip(x[1], x[0])])
        return matrix

    def fill_savings_matrix_ip(self, on1, on2):  # ON STANDS FOR ORDER NR.
        '''FUNCTION CALCULATES SAVINGS WHEN COMBINING TWO BOXES'''
        if on1 == on2:
            saving = np.nan
        else:
            nodelist1 = self.boxes.at[on1, 'nodelist']
            nodelist2 = self.boxes.at[on2, 'nodelist']

            nl_combined = nodelist1 + nodelist2
            nl_sorted = self.refpos + self.sorter.sort(nl_combined) + self.refpos

            dist_combined = self.distance(nl_sorted)
            dist_sep = self.boxes.at[on1, 'distance_per_box'] + self.boxes.at[on2, 'distance_per_box']
            saving = dist_sep - dist_combined
        return saving

    def savings_list_ip(self, savings_matrix):
        SL = []
        savings_matrix.applymap(lambda x: SL.append(x))
        SL.sort()
        SAVINGS_LIST = list(SL for SL, _ in itertools.groupby(SL))
        SAVINGS_LIST = pd.DataFrame(SAVINGS_LIST, columns=['box1', 'box2', 'saving'])
        SAVINGS_LIST.dropna(inplace=True)
        # SAVINGS_LIST.sort_values('box2', ascending=False, inplace=True)
        # SAVINGS_LIST.sort_values('box1', ascending=False, inplace=True)
        SAVINGS_LIST.sort_values('saving', ascending=False, inplace=True)
        SAVINGS_LIST.reset_index(inplace=True, drop=True)
        return SAVINGS_LIST

    def create_initial_pair(self, savings_list):
        '''STARTS BATCHING BY PLACING THE FIRST TWO ITEMS IN A BATCH'''
        ord1 = savings_list.at[0, 'box1']
        ord2 = savings_list.at[0, 'box2']
        batch = {ord1: self.boxes.at[ord1, 'nodelist'], ord2: self.boxes.at[ord2, 'nodelist']}
        self.boxes_to_batch.remove(ord1)
        self.boxes_to_batch.remove(ord2)
        return batch

    def savings_matrix_eb(self, batch):
        '''SAVINGS LIST FOR EXISTING BATCHES'''
        nodelist_batch = list(itertools.chain(*batch.values()))
        savings_matrix = pd.DataFrame(data=self.boxes_to_batch, index=self.boxes_to_batch, columns=['saving'])
        savings_matrix = savings_matrix.applymap(
            lambda x: self.fill_savings_matrix_eb(nodelist_batch, x)).sort_values(
            by=['saving'], ascending=False)
        return savings_matrix

    def fill_savings_matrix_eb(self, nodelist_batch, on):
        '''FILL SAVINGS MATRIX FOR EXISTING BATCHES WITH SAVINGS THAT CORRESPOND TO THE ORDERS'''
        nodelist1 = nodelist_batch
        nodelist2 = self.boxes.at[on, 'nodelist']

        nl_combined = nodelist1 + nodelist2
        nl_sorted = self.refpos + self.sorter.sort(nl_combined) + self.refpos

        dist_combined = self.distance(nl_sorted)
        dist_sep = self.boxes.at[on, 'distance_per_box'] + self.distance(nodelist1)
        return dist_sep - dist_combined

    def add_to_batch(self, savings_matrix):
        order_to_add = savings_matrix.first_valid_index()
        self.boxes_to_batch.remove(order_to_add)
        return order_to_add


#%%


if __name__ == "__main__":
    print('Loading Data...')
    raw_data = pd.read_excel('Data/ups_pick_data_may_june_preprocessed.xlsx')
    distance_matrix = pd.read_excel('Data/distance_matrix_UPS_AB_AX.xlsx', index_col=0)
    capacity = 9
    refpos = [(27, -1)]
    date = '2019-05-01'
    input_date = date + ' 00:00:00'
    batch = Batch(raw_data, distance_matrix, capacity, refpos, 'UPS', input_date)
    batch.batch()
