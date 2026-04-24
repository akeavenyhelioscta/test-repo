"""Forward-only analog forecaster (tomorrow-setup KNN)."""

from da_models.forward_only_knn.pipelines.forecast import run_forecast
from da_models.forward_only_knn.pipelines.strip_forecast import run_strip_forecast

__all__ = ["run_forecast", "run_strip_forecast"]
