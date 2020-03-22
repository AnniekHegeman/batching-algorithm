import pandas as pd
import numpy as np
from sorter import SortNodesFactory
from batcher import Batch
import itertools 


class PerformanceCalculator:
    '''PerformanceCalculator class takes '''

    def __init__(self, raw_picking_data: pd.DataFrame, pick_runs_algorithm: list,
                 dist_matrix: pd.DataFrame, refpos: list, date: str, cust_name: str):
        self.raw_picking_data = raw_picking_data
        self.pick_runs_actual = []
        self.pick_runs_algorithm = pick_runs_algorithm
        self.distance_matrix = dist_matrix
        self.refpos = refpos
        self.date = date
        self.sorter = SortNodesFactory.create(cust_name)
        self.improvement = {}

    
    def determine_performance(self):
        self.preprocess_picking_data()
        self.calculate_actual_distance()
        actual_distance = list(self.pick_runs_actual['distance'].values)
        algorithm_distance = self.calculate_algorithm_distance()
        self.improvement = {'actual_distance': sum(actual_distance),
                            'algorithm_distance': sum(algorithm_distance),
                            '%_decrease': ((sum(actual_distance)-sum(algorithm_distance))/sum(actual_distance))*100,
                            'actual_mean': np.mean(actual_distance),
                            'algorithm_mean': np.mean(algorithm_distance),
                            'actual_median': np.median(actual_distance),
                            'algorithm_median': np.median(algorithm_distance),
                            'actual_max': np.max(actual_distance),
                            'actual_min': np.min(actual_distance),
                            'actual_nr_of_rounds': len(actual_distance),
                            'algorithm_max': np.max(algorithm_distance),
                            'algorithm_min': np.min(algorithm_distance),
                            'algorithm_nr_of_rounds': len(algorithm_distance)}

    def preprocess_picking_data(self):
        selected_data = self.raw_picking_data.copy()
        selected_data = selected_data[selected_data['DAY_DATE'] == self.date]
        selected_data['nodelist'] = tuple(zip(selected_data['Aisle'], selected_data['Bay']))
        selected_data['colunique'] = selected_data['DAY_DATE'].astype(str) + '_' + selected_data['USER_ID'].astype(
            str) + '_' + selected_data['WAVE_NBR'].astype(str)
        pick_runs = selected_data.groupby('colunique')['nodelist'].apply(set).apply(list)
        self.pick_runs_actual = pd.DataFrame(pick_runs)


    def calculate_actual_distance(self):
        NESTED_NODELIST = list(self.pick_runs_actual['nodelist'])
        self.pick_runs_actual['sorted_nodes'] = [self.refpos + self.sorter.sort(nodelist) + self.refpos for nodelist in NESTED_NODELIST]
        self.pick_runs_actual['distance'] = self.pick_runs_actual['sorted_nodes'].apply(lambda x: self.distance(x))  
        
    def calculate_algorithm_distance(self):
        distance_per_batch = []
        for batch in self.pick_runs_algorithm:
            nodelist = list(itertools.chain(*batch.values()))
            sorted_nodelist = self.refpos + self.sorter.sort(nodelist) + self.refpos
            distance = self.distance(sorted_nodelist)
            distance_per_batch.append(distance)
        return distance_per_batch
    
    def distance(self, nodelist):
        ''' FUNCTION RETRIEVES DISTANCES FROM THE DISTANCE MATRIX '''
        distance = 0
        for i in range(len(nodelist)-1):
            dist = self.distance_matrix.at[str(nodelist[i]), str(nodelist[i+1])]
            if np.isnan(dist):
                dist = self.distance_matrix.get_value(nodelist[i+1], nodelist[i])
            distance += dist
        return distance        
    

if __name__ == '__main__':

    pick_data = pd.read_excel('Data/ups_pick_data_may_june_preprocessed.xlsx')
    distance_matrix = pd.read_excel('Data/distance_matrix_UPS_AB_AX.xlsx', index_col=0)
    refpos = [(27, -1)]
    capacity = 9
    date = '2019-05-01'
    input_date = date + ' 00:00:00'

    batch = Batch(pick_data, distance_matrix, capacity, refpos, 'UPS', input_date)
    batch.batch()
    batches = batch.batches

    performance_calculator = PerformanceCalculator(raw_picking_data=pick_data, pick_runs_algorithm=batches,
                                                   dist_matrix=distance_matrix, refpos=refpos, date=input_date,
                                                   cust_name='UPS')
    performance_calculator.determine_performance()
    results = performance_calculator.improvement



