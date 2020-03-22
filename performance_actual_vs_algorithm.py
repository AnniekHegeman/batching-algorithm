import pandas as pd
from performance_calculator import PerformanceCalculator
from batcher import Batch
from concurrent.futures import ProcessPoolExecutor

raw_data = pd.read_excel('Data/Batching historical picks_2019 December_preprocessed.xlsx')
distance_matrix = pd.read_excel('Data/distance_matrix_UPS_AB_AX.xlsx', index_col=0)
capacity = 9
refpos = [(27, -1)]
dates = list(raw_data.DAY_DATE.unique())

results_per_day = {}
results_per_day_2 = []

def run_performance_calculator(date):
    batch = Batch(raw_data, distance_matrix, capacity, refpos, 'UPS', date)
    batch.batch()
    batches = batch.batches

    performance_calculator = PerformanceCalculator(raw_picking_data=raw_data, pick_runs_algorithm=batches,
                                                    dist_matrix=distance_matrix, refpos=refpos, date=date,
                                                    cust_name='UPS')

    performance_calculator.determine_performance()
    results = performance_calculator.improvement
    results_per_day[date] = results
    return results


def main():
    with ProcessPoolExecutor(max_workers=6) as executor:
        results = executor.map(run_performance_calculator, dates)
    for result in results:
        results_per_day_2.append(result)

if __name__ == '__main__':
   main()

