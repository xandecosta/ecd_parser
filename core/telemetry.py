import time
from typing import Dict, Any, Callable, TypeVar, cast
from functools import wraps


class TelemetryCollector:
    """
    Centraliza a coleta de métricas de performance por componente e método.
    """

    def __init__(self):
        self.data: Dict[str, Dict[str, Any]] = {}
        self.session_start = time.time()
        self.global_stats: Dict[str, Dict[str, float]] = {}

    def start_ecd(self, ecd_id: str):
        """Inicializa a coleta para uma específica ECD (Ano)."""
        if ecd_id not in self.data:
            self.data[ecd_id] = {"inicio": time.time(), "termino": None, "metrics": {}}

    def end_ecd(self, ecd_id: str):
        """Finaliza a coleta para uma específica ECD."""
        if ecd_id in self.data:
            self.data[ecd_id]["termino"] = time.time()

    def record_metric(self, ecd_id: str, component: str, method: str, duration: float):
        """Registra o tempo de execução de um método de componente."""
        if ecd_id not in self.data:
            self.start_ecd(ecd_id)

        if component not in self.data[ecd_id]["metrics"]:
            self.data[ecd_id]["metrics"][component] = {}

        self.data[ecd_id]["metrics"][component][method] = duration

    def record_global(self, component: str, method: str, duration: float):
        """Registra métricas para processos globais (pós-processamento)."""
        if component not in self.global_stats:
            self.global_stats[component] = {}
        self.global_stats[component][method] = duration

    def get_ecd_metrics(self, ecd_id: str) -> Dict[str, Any]:
        return self.data.get(ecd_id, {})

    def merge(self, other_data: Dict[str, Any]) -> None:
        """
        Funde dados de telemetria de processos paralelos neste coletor.
        Utilizado pelo orquestrador principal para consolidar resultados.
        """
        if other_data:
            self.data.update(other_data)


F = TypeVar("F", bound=Callable[..., Any])


def monitor_task(component_name: str, method_name: str) -> Callable[[F], F]:
    """
    Decorator para medir o tempo de execução de métodos e registrar no TelemetryCollector.
    Requer que a instância da classe tenha um atributo 'telemetry' ou receba um.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Tenta encontrar o coletor na instância ou nos argumentos
            telemetry = getattr(self, "telemetry", None)
            ecd_id = getattr(self, "current_ecd_id", "GLOBAL")

            start = time.time()
            result = func(self, *args, **kwargs)
            end = time.time()

            if telemetry:
                if ecd_id == "GLOBAL":
                    telemetry.record_global(component_name, method_name, end - start)
                else:
                    telemetry.record_metric(
                        ecd_id, component_name, method_name, end - start
                    )
            return result

        return cast(F, wrapper)

    return decorator
