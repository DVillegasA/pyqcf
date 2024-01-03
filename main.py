from typing import Iterable
from pymongo import MongoClient
from pyqcf import market_data as market_data
import qcfinancial as qcf
import private.config as config

class MarketData(market_data.MarketDataSource):
    def __init__(self):
        # Recibir conexión a MongoDB
        client = MongoClient(f"mongodb+srv://{config.user_mongodb}:{config.password_mongodb}@{config.host_mongodb}/?retryWrites=true&w=majority")
        db = client['r07_db']
        collection_fx = db["fx_rates"]
        collection_index = db["historical_indexes"]
        
        fx_names = collection_fx.distinct(key='fx_rate')
        index_names = collection_index.distinct(key='index_name')
        
        self.fx_rates = dict()
        self.index_rates = dict()

        for fx_name in fx_names:
            ts = qcf.time_series()
            for data in collection_fx.find({'fx_rate':fx_name}):
                process_date = data['process_date']
                ts[qcf.QCDate(process_date.day, process_date.month, process_date.year)] = data['value']
            self.fx_rates[fx_name] = ts

        for index_name in index_names:
            ts = qcf.time_series()
            for data in collection_index.find({'index_name':index_name}):
                process_date = data['process_date']
                ts[qcf.QCDate(process_date.day, process_date.month, process_date.year)] = data['value']
            self.index_rates[index_name] = ts

        client.close()

        # Generar los calendarios
        self.calendars = dict()
        self.calendars['SCL'] = qcf.BusinessCalendar(qcf.QCDate(1,1,2023), 2)
        self.calendars['NY'] = qcf.BusinessCalendar(qcf.QCDate(1,1,2023), 2)
        self.calendars['LONDON'] = qcf.BusinessCalendar(qcf.QCDate(1,1,2023), 2)

    def get_calendars(self) -> dict[str, qcf.BusinessCalendar]:
        return self.calendars
    
    def get_index_values(self, initial_date, end_date, index_names: Iterable[str]) -> dict[str, qcf.time_series]:
        if len(index_names) == 0:
            raise ValueError("index_name array empty. You need to provide at least one FX or Index Name.")

        initial_date_qcf = qcf.build_qcdate_from_string(initial_date)
        end_date_qcf = qcf.build_qcdate_from_string(end_date)

        result_index_values = dict()

        for name in index_names:
            if name in self.fx_rates:
                data = self.fx_rates[name]
            elif name in self.index_rates:
                data = self.index_rates[name]
            else:
                raise ValueError(f"{name} is neither a valid FX Name or Index Name.")

            ts = qcf.time_series()
            for process_date, value in data.items():
                if process_date >= initial_date_qcf and process_date <= end_date_qcf:
                    ts[process_date] = value
            result_index_values[name] = ts

        return result_index_values

def main():
    mongo_md = MarketData()
    calendars = mongo_md.get_calendars()
    fx_rates = mongo_md.get_index_values('2023-01-01', '2023-12-31', ['SOFR_INDEX', 'SOFRTERM_1Y', 'USDCLP'])
    
    print(calendars)
    print(fx_rates)

if __name__ == '__main__':
    main()