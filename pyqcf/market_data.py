from datetime import date
from abc import ABC, abstractmethod
from math import exp
from typing import (
    Dict,
    Tuple,
    List,
    Iterable,
)
import pandas as pd
from pydantic import (
    BaseModel,
    PrivateAttr,
    ConfigDict,
)

import qcfinancial as qcf
from . import wrappers as qcw


class MarketDataSource(ABC):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def get_calendars(self, *args, **kwargs) -> dict[str, qcf.BusinessCalendar]:
        pass

    @abstractmethod
    def get_index_values(
            self,
            initial_date,
            end_date,
            index_names: Iterable[str],
    ) -> dict[str, qcf.time_series]:
        pass

    def get_index_value_for_date(
            self,
            which_date: date,
            index_names: str,
    ) -> float:
        pass


class CurveHandler(BaseModel):
    """
    Almacena los valores y representación qcf de las curvas cero cupón y permite aplicar escenarios a las curvas.

    Attributes:
        process_date (date): Fecha a la que se efectuará la valorización.

        zero_coupon_curves (Dict[str, Tuple[pd.DataFrame, qcf.ZeroCouponCurve]]): `Dict` con las curvas cero cupón
        requeridas en la valorización. La `key` del `Dict` es el código de la curva. Los `values` del `dict` son:

        - una `tuple` con un `DataFrame` que contiene los plazos en días y tasas de la curva. Estas columnas deben
        llamarse "tenor" y "value" respectivamente,
        - el objeto qcf.ZeroCouponCurve que representa la curva.

        Las curvas corresponden a la fecha `process_date`.
    """
    process_date: date
    zero_coupon_curves: Dict[str, Tuple[pd.DataFrame, qcf.ZeroCouponCurve]]
    _scenario: Dict[str, Tuple[pd.DataFrame, qcf.ZeroCouponCurve]] = PrivateAttr()

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    def __init__(self, **data):
        super().__init__(**data)
        self._scenario = self.zero_coupon_curves.copy()

    def __getitem__(self, item):
        return self.zero_coupon_curves[item]

    def add_parallel_shift(self, shift: float, curves: List[str]) -> None:
        """
        Suma un movimiento paralelo a todos los puntos de las curvas indicadas. Las curvas con el escenario aplicado se
        obtienen con el método `get_scenario`.

        Parameters
        ----------
        shift: float
            Movimiento a aplicar expresado en número. Por ejemplo, 1 pb = .0001

        curves: List[str]
            Códigos de las curvas a las que se aplicará el movimiento. Si un código no existe se levantará una
            excepción.

        Returns
        -------
        None

        """
        for crv in self.zero_coupon_curves:
            if crv in curves:
                df = self.zero_coupon_curves[crv][0].copy()
                df["value"] += shift
                qcf_ir = self.zero_coupon_curves[crv][1].get_qc_interest_rate_at(0)
                new_crv = build_zero_coupon_curve(
                    df['tenor'],
                    df['value'],
                    qcf_ir.get_year_fraction(),
                    qcf_ir.get_wealth_fraction(),
                )
                self._scenario[crv] = df, new_crv
            else:
                self._scenario[crv] = (
                    self.zero_coupon_curves[crv][0],
                    self.zero_coupon_curves[crv][1],
                )

    def move_tenor_of_curve(self, shift: float, index: int, curve: str) -> None:
        """
        Suma un movimiento a un tenor (vértice) de una curva. Las curvas con el escenario aplicado se obtienen con el
        método `get_scenario`.

        Parameters
        ----------
        shift: float
            Movimiento a aplicar expresado en número. Por ejemplo, 1 pb = .0001

        index: str
            Tenor de la curva a la que se aplicará el movimiento. Si el tenor no existe se levantará una excepción.

        curve: str
            Código de la curva a la que se aplicará el movimiento. Si un código no existe se levantará una excepción.

        Returns
        -------
        None

        """
        for crv in self.zero_coupon_curves:
            if crv == curve:
                df = self.zero_coupon_curves[curve][0].copy()
                df.at[index, "value"] += shift
                qcf_ir = self.zero_coupon_curves[curve][1].get_qc_interest_rate_at(0)
                new_crv = build_zero_coupon_curve(
                    df['tenor'],
                    df['value'],
                    qcf_ir.get_year_fraction(),
                    qcf_ir.get_wealth_fraction(),
                )
                self._scenario[curve] = df, new_crv
            else:
                self._scenario[crv] = (
                    self.zero_coupon_curves[crv][0],
                    self.zero_coupon_curves[crv][1],
                )

    def apply_additive_scenario(self, scenarios: Dict[str, Dict[str, float]]) -> None:
        """
        Aplica escenarios aditivos a las curvas. Si el largo del escenario es distinto del largo de la curva se
        levanta una excepción.

        Parameters
        ----------
        scenarios: Dict[str, Dict[str, float]]
            La primera `key` es el nombre de la curva. La `key` del `dict` interno es un `str` que representa un
            tenor de la curva

        Returns
        -------
        None
            Las curvas con el escenario aplicado se obtienen con el método `get_scenario`.
        """
        for curve in self.zero_coupon_curves:
            if curve in scenarios:
                scenario = scenarios[curve]
                if len(scenario) != len(self.zero_coupon_curves[curve][0]):
                    raise ValueError(
                        f"El escenario para la curva {curve} tiene un número distinto de vértices que la curva."
                    )
                to_add = [
                    element[1]
                    for element in sorted(
                        scenario.items(),
                        key=lambda item: qcw.build_tenor_from_str(item[0]),
                    )
                ]
                df = self.zero_coupon_curves[curve][0].copy()
                df["value"] += to_add
                qcf_ir = self.zero_coupon_curves[curve][1].get_qc_interest_rate_at(0)
                new_crv = build_zero_coupon_curve(
                    df['tenor'],
                    df['value'],
                    qcf_ir.get_year_fraction(),
                    qcf_ir.get_wealth_fraction(),
                )
                self._scenario[curve] = df, new_crv
            else:
                self._scenario[curve] = (
                    self.zero_coupon_curves[curve][0],
                    self.zero_coupon_curves[curve][1],
                )

    def get_scenario(self) -> Dict[str, Tuple[pd.DataFrame, qcf.ZeroCouponCurve]]:
        """
        Retorna la variable `_scenario`. En esta variable se encuentra almacenada la última curva obtenida con alguno
        de los métodos que modifican las curvas base.
        """
        return self._scenario


class FxRateIndexHandler(BaseModel):
    """
    Almacena los valores de índices de tipo de cambio a una fecha. Puede construir escenarios para esos índices.

    Parameters
    ----------
    process_date: date
        Fecha a la que se refieren los datos

    fx_rate_index_values: Dict[str, float]
        Valores de los índices FX a `process_date`.
    """

    process_date: date
    fx_rate_index_values: Dict[str, float]
    _scenario: Dict[str, float] = PrivateAttr()

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(self, **data):
        super().__init__(**data)
        self._scenario = {k: v for k, v in self.fx_rate_index_values.items()}

    def apply_exp_scenario(self, scenario: Dict[str, float]) -> None:
        """
        Aplica un escenario de retornos exponenciales.

        Parameters
        ----------
        scenario: Dict[str, float]
            Retorno exponencial a aplicar a cada valor de índice FX. La `key` del `Dict` es el código del índice FX.
            Si alguno de los índices no está en el `Dict` se asume que el retorno es 0.

        Returns
        -------
        None:
            Los índices con el último escenario aplicado se obtienen con el método `get_fx_rate_index_value`.
        """
        for key, value in self.fx_rate_index_values.items():
            retorno = scenario.get(key, 0.0)
            self._scenario[key] = value * exp(retorno)

    def get_fx_rate_index_value(
            self, fx_rate_index_code: str,
            use_scenario: bool = False
    ) -> float:
        """
        Retorna el valor de un FX Rate Index.

        Parameters
        ----------
        fx_rate_index_code: str
            Código del Fx Rate Index cuyo valor se desea obtener.

        use_scenario: bool
            Si True usa el valor del último escenario calculado, si False se utiliza los valores base.

        Returns
        -------
        float:
            Valor del índice solicitado.
        """
        if fx_rate_index_code not in self.fx_rate_index_values:
            raise ValueError(f"Código {fx_rate_index_code} no se encuentra.")
        if use_scenario:
            return self._scenario[fx_rate_index_code]
        else:
            return self.fx_rate_index_values[fx_rate_index_code]


# class AmountToClp(BaseModel):
#     process_date: date
#     market_data: MarketDataSource
#     config: dict[str, str]
#     """
#     Permite convertir un monto en una moneda a un monto en CLP.
    
#     Parameters
#     ----------
#     process_date: date
#         Fecha a la cual se realiza la conversión.
    
#     market_data: MarketDataSource
#         Fuente de los datos de mercado
    
#     config: dict[str, str]
#         Dict cuyos `keys` son los nombres de las monedas y cuyos `values` son los nombres de los índices de tipo de 
#         cambio que se deben utilizar para cada moneda. Todos deben ser del tipo MXXCLP (la moneda fuerte debe ser la 
#         moneda distinta de CLP).
    
#     """

#     def __call__(self, amount: float, currency: qcw.Currency):
#         """
#         Transforma un monto en cualquier moneda a un monto en CLP. Para la conversión utilizará el tipo de cambio
#         individualizado en config a `process_date`.

#         Parameters
#         ----------
#         amount: float
#             Monto a convertir.

#         currency: qcw.Currency
#             Moneda del monto a convertir.


#         Returns
#         -------
#         float
#             El resultado de la conversión.

#         """
#         index_code = self.config[currency.value]
#         index_value = self.market_data.get_index_value_for_date(
#             self.process_date,
#             index_code,
#             )

#         return amount * index_value


def build_zero_coupon_curve(
        plazos: Iterable[int],
        tasas: Iterable[float],
        yf: qcw.YearFraction,
        wf: qcw.WealthFactor,
        interpolator=None,
) -> qcf.ZeroCouponCurve:
    """
    Construye un objeto de tipo `ZeroCouponCurve`.

    Args:
        plazos (Iterable[int]): plazos de la curva
        tasas (Iterable[float]): tasas de la curva
        yf (qcw.YearFraction): fracción de año utilizada en la convención de las tasas
        wf (qcw.WealthFactor): factor de capitalización utilizado en la convención de las tasas
        interpolator (None): NO UTILIZAR, NO ESTÁ IMPLEMENTADO

    Returns: qcf.ZeroCouponCurve

    """
    _plazos = qcf.long_vec()
    for p in plazos:
        _plazos.append(int(p))

    _tasas = qcf.double_vec()
    for t in tasas:
        _tasas.append(t)

    return qcf.ZeroCouponCurve(
        qcf.QCLinearInterpolator(qcf.QCCurve(_plazos, _tasas)),
        qcf.QCInterestRate(
            0.0,
            yf.as_qcf(),
            wf.as_qcf(),
        ),
    )
