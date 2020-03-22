import pandas as pd
import numpy as np
import time
from sorter import SortNodesFactory
import itertools
import random


# %%

class Batch:

    def __init__(self, boxes, dist_matrix, capacity: list, refpos: list, cust_name: str):
        self.boxes = boxes
        self.distance_matrix = dist_matrix
        self.capacity = sorted(capacity, reverse=True)
        self.refpos = refpos
        self.boxes_to_batch = {}
        self.savings_matrix = []
        self.sorter = SortNodesFactory.create(cust_name)
        self.iteration_counter = 0

    def run_batching(self):

        '''FUNCTION BATCHES ORDERS TOGETHER (BOXEX IN UPS CASE
        INPUT: DATAFRAME WITH ORDER_ID AND NODELIST; CAPACITY CONSTRAINT, DISTANCE MATRIX, REFFERENCE POSITION
        OUTPUT: LIST OF DICTIONARIES, EACH DICTIONARY IS A BATCH; keys=ORDER_ID, values=LOCATIONS'''

        batches = []

        NESTED_NODELIST = list(self.boxes['nodelist'])
        self.boxes['sorted_nodes'] = [self.refpos + self.sorter.sort(nodelist) + self.refpos for nodelist in
                                      NESTED_NODELIST]
        self.boxes['distance_per_box'] = self.boxes['sorted_nodes'].apply(lambda x: self.distance(x))
        self.boxes_to_batch = self.boxes['size'].to_dict()

        self.savings_matrix = self.generate_savings_matrix_ip()

        # KEEP BATCHING WHILE STILL BOXES LEFT
        while len(self.boxes_to_batch) > 0:
            print('loop1')
            self.iteration_counter = 0
            self.active_capacity = self.capacity[self.iteration_counter]

            try:  # GENERATE SAVINGS_LIST(matrix) TO PICK INITIAL PAIR AND ADD INITIAL PAIR TO BATCH
                savings_list = self.savings_list_ip(self.savings_matrix)
                batch = self.create_initial_pair(savings_list)  # dtype=dict: key=boxnr, value=nodelist
            except:
                batch = {
                    list(self.boxes_to_batch.keys())[0]: self.boxes.at[list(self.boxes_to_batch.keys())[0], 'nodelist']}
                batches.append(batch)
                break

            # KEEP ADDING TO BATCH WHILE NOT ALL LEVELS ARE FILLED AND BOXES LEFT
            while self.iteration_counter < len(self.capacity) and len(self.boxes_to_batch) > 0:
                print('loop2')
                if self.iteration_counter > 0:
                    self.active_capacity = self.capacity[self.iteration_counter]
                else:
                    pass

                # KEEP BATCHING WHILE LEVEL NOT FULL AND BOXES LEFT
                while self.active_capacity >= 0 and len(self.boxes_to_batch) > 0:
                    print('loop3')
                    try:
                        savings_mat = self.savings_matrix_eb(batch)  # MATRIX['BOX', 'SAVING', 'SIZE']
                        order_to_add = self.add_to_batch(savings_mat)  # SELECT BOX AND REMOVE FROM BOXES TO BATCH
                        batch[order_to_add] = self.boxes.at[order_to_add, 'nodelist']  # ADD NEW BOX TO BATCH DICTIONARY
                    except:
                        self.iteration_counter += 1
                        break

            batch_orders = list(batch.keys())
            self.savings_matrix.drop(batch_orders, axis=0, inplace=True)
            self.savings_matrix.drop(batch_orders, axis=1, inplace=True)
            batches.append(batch)

        return batches

    # ------------------------------------------------------------------------------------------------------------------
    # HELPER FUNCTIONS
    # ------------------------------------------------------------------------------------------------------------------

    def distance(self, nodelist):
        ''' FUNCTION RETRIEVES DISTANCES FROM THE DISTANCE MATRIX '''
        distance = 0
        for i in range(len(nodelist) - 1):
            dist = self.distance_matrix.at[str(nodelist[i]), str(nodelist[i + 1])]
            if np.isnan(dist):
                dist = self.distance_matrix.get_value(str(nodelist[i + 1]), str(nodelist[i]))
            distance += dist
        return distance

    def generate_savings_matrix_ip(self):
        '''GENERATE SAVINGS MATRIX AND FILL WITH FILL SAVINGS MATRIX FUNCTION'''
        matrix = pd.DataFrame(
            data=[[{'box1': max(x, y), 'box2': min(x, y), 'size': self.boxes_to_batch[x] + self.boxes_to_batch[y]}
                   for x in self.boxes_to_batch.keys()] for y in self.boxes_to_batch.keys()],
            index=self.boxes_to_batch.keys(), columns=self.boxes_to_batch.keys()).applymap(
            lambda x: self.fill_savings_matrix_ip(x))
        return matrix

    def fill_savings_matrix_ip(self, dictionary):
        dict_new = dictionary
        dict_new['saving'] = self.calculate_saving_ip(dict_new['box1'], dict_new['box2'])
        return dict_new

    def calculate_saving_ip(self, on1, on2):  # ON STANDS FOR ORDER NR.
        '''FUNCTION CALCULATES SAVINGS WHEN COMBINING TWO BOXES'''
        if int(on1) == int(on2):
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
        savings_list = pd.DataFrame(SL)
        savings_list['size'] = savings_list['size'].apply(lambda x: self.capacity_constraint(x))
        savings_list.dropna(inplace=True)
        savings_list.drop_duplicates(inplace=True)
        savings_list.sort_values(['saving', 'size'], ascending=[False, True], inplace=True)
        savings_list.reset_index(inplace=True)
        return savings_list

    def capacity_constraint(self, value):
        if value > self.active_capacity:
            return np.nan
        else:
            return value

    def create_initial_pair(self, savings_list):
        '''STARTS BATCHING BY PLACING THE FIRST TWO ITEMS IN A BATCH'''
        ord1 = {'on': savings_list.at[0, 'box1'], 'size': self.boxes.at[savings_list.at[0, 'box1'], 'size']}
        ord2 = {'on': savings_list.at[0, 'box2'], 'size': self.boxes.at[savings_list.at[0, 'box2'], 'size']}
        initial_pair = {ord1['on']: self.boxes.at[ord1['on'], 'nodelist'],
                        ord2['on']: self.boxes.at[ord2['on'], 'nodelist']}
        del self.boxes_to_batch[ord1['on']]
        del self.boxes_to_batch[ord2['on']]
        self.reduce_capacity(ord1['size'] + ord2['size'])
        return initial_pair

    def reduce_capacity(self, size):
        self.active_capacity -= size

    def savings_matrix_eb(self, batch):
        nodelist_batch = list(itertools.chain(*batch.values()))
        boxes_to_batch = list(self.boxes_to_batch.keys())
        savings_matrix = pd.DataFrame(data=boxes_to_batch, index=boxes_to_batch, columns=['saving'])
        savings_matrix['size'] = savings_matrix['saving'].apply(lambda x: self.boxes.at[str(x), 'size'])
        savings_matrix['size'] = savings_matrix['size'].apply(lambda x: self.capacity_constraint(x))
        savings_matrix.dropna(inplace=True)
        savings_matrix['saving'] = savings_matrix['saving'].apply(
            lambda x: self.fill_savings_matrix_eb(nodelist_batch, x))
        savings_matrix.sort_values(by=['saving'], ascending=False, inplace=True)
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
        del self.boxes_to_batch[order_to_add]
        self.reduce_capacity(savings_matrix.at[order_to_add, 'size'])
        return order_to_add


# %%

capacity = [980, 980, 980]  # mm
refpos = [(27, -1)]

data = pd.read_excel('Data/ups_pick_data_may_june_preprocessed.xlsx')
selected_data = data[data['DAY_DATE'] == '2019-05-01 00:00:00'][
    ['WAVE_NBR', 'CNTR_NBR', 'LOCN_BRCD', 'Aisle', 'Bay']].copy()
selected_data['nodelist'] = tuple(zip(selected_data['Aisle'], selected_data['Bay']))
dist_matrix = pd.read_excel('Data/distance_matrix_UPS_AB_AX.xlsx', index_col=0)

box_sizes = pd.read_excel('Data/box_dimensions.xlsx')
size_list = list(box_sizes['Length (mm)'].values)

boxes = pd.DataFrame(selected_data.groupby('CNTR_NBR')['nodelist'].apply(set).apply(list))
boxes.index = boxes.index.map(str)
boxes['size'] = random.choice(size_list)

batch = Batch(boxes, dist_matrix, capacity, refpos, "UPS")
batches_test = batch.run_batching()

len(capacity)
