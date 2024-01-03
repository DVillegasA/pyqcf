from typing import Iterable
from pymongo import MongoClient
from pyqcf import market_data as market_data
from datetime import datetime
import qcfinancial as qcf
import private.config as config

class MarketData(market_data.MarketDataSource):
    def __init__(self):
        # Se realiza la conexión con MongoDB
        # Los parámetros de conexión deben especificarse en el archivo private/config.py
        client = MongoClient(f"mongodb+srv://{config.user_mongodb}:{config.password_mongodb}@{config.host_mongodb}/?retryWrites=true&w=majority")
        db = client['r07_db']
        collection_fx = db["fx_rates"]
        collection_index = db["historical_indexes"]
        
        # Primero recuperamos los nombres de los FX e Indices en MongoDB, para seguir la convención enforzada por la clase abstracta
        fx_names = collection_fx.distinct(key='fx_rate')
        index_names = collection_index.distinct(key='index_name')
        
        # Inicializamos los diccionarios que contendran la información de FX e Indices
        self.fx_rates = dict()
        self.index_rates = dict()

        # Mapeamos cada nombre recuperado a una serie de tiempo de QcF que contiene los valores guardados en DB
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

        # Generar los calendarios
        current_datetime = datetime.now()
        self.calendars = dict()
        self.calendars['SCL'] = qcf.BusinessCalendar(qcf.QCDate(current_datetime.day,current_datetime.month,current_datetime.year), 2)
        self.calendars['NY'] = qcf.BusinessCalendar(qcf.QCDate(current_datetime.day,current_datetime.month,current_datetime.year), 2)
        self.calendars['LONDON'] = qcf.BusinessCalendar(qcf.QCDate(current_datetime.day,current_datetime.month,current_datetime.year), 2)

        # Cerramos la conexión a base de datos
        client.close()

    def get_calendars(self) -> dict[str, qcf.BusinessCalendar]:
        """
        Recupera todos los calendarios dados de alta en la base de datos
        :return: dict[str, qcf.BusinessCalendar], diccionadio con objectos de calendario QcFinancial con un plazo de 2 años para cada calendario
        """
        return self.calendars
    
    def get_index_values(self, initial_date, end_date, index_names: Iterable[str]) -> dict[str, qcf.time_series]:
        """
        Recupera series de tiempo con los datos diarios para los nombres de FX e Indices solicitados en el intervalo de tiempo pedido
        :param initial_date: str, cota inferior para las fechas de los datos
        :param end_date: str, cota superior para las fechas de los datos
        :param index_names: Iterable[str], lista de nombres de FX e Indices a recuperar
        :return: dict[str, qcf.time_series], diccionario con series de tiempo con los resultados para cada uno de los nombres solicitados
        """
        # Primero revisamos si index_names contiene registros
        if len(index_names) == 0:
            raise ValueError("Arreglo index_names vacio. Debe especificar el nombre de al menos un FX o Indice.")

        # Convertimos las fechas de inicio y termino entregadas por el usuario a formato QcF
        initial_date_qcf = qcf.build_qcdate_from_string(initial_date)
        end_date_qcf = qcf.build_qcdate_from_string(end_date)

        # Se crea el diccionario de resultados a retornar
        result_index_values = dict()

        for name in index_names:
            # Para cada nombre suministrado, recuperamos su información dependiendo de si es un FX o un Indice
            # En caso de que el nombre suministrado no existe, se levanta una alerta al usuario
            if name in self.fx_rates:
                data = self.fx_rates[name]
            elif name in self.index_rates:
                data = self.index_rates[name]
            else:
                raise ValueError(f"{name} is neither a valid FX Name or Index Name.")

            # Luego creamos una nueva serie de tiempo, incluyendo solo valores entre el rango de fechas suministrado
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