"""This module is automatically generated from the JSON schemas available at:

    https://github.com/statisticsnorway/arkitektur-informasjonsmodeller/tree/main/process-data

It contains pydantic models generated using the `pydantic` tool `datamodel-code-generator`
and the following command::

    poetry run datamodel-codegen --output-model-type pydantic_v2.BaseModel
    --use-standard-collections --use-union-operator
    --input quality-control-result-json-schema.json --output quality_control_result.py

Minor modifications were made to the generated code after creation.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class QualityControlResults(Enum):
    """Quality control results."""

    field_0 = "0"
    """Quality ok."""

    field_1 = "1"
    """Quality issues detected."""

    field_2 = "2"
    """Missing value detected."""


class QualityControlResult(BaseModel):
    """Pydantic model statistics quality control result."""

    statistics_name: str = Field(
        ..., description="Statistics shortname or statistics product name"
    )
    quality_control_id: str = Field(
        ..., description="A reference (or link/uri) to the quality control description"
    )
    data_location: list[str] = Field(
        ...,
        description="Controlled dataset reference/filepath (eg. GCS-path to a parquet file) or other dataset reference (eg. ref. to a CloudSQL database table).",
    )
    data_period: str = Field(
        ..., description="Data period controlled - eg. year, date, date-time, ..."
    )
    quality_control_datetime: datetime = Field(
        ..., description="Quality control datetime (date and time, ISO 8601)"
    )
    quality_control_results: QualityControlResults = Field(
        ...,
        description="Quality control result: quality ok (0), quality issues detected (1), missing value detected (3)",
    )
    quality_control_run_exception: str | None = Field(
        None,
        description="Exception description. An error or warning occurred when executing the quality control routine.",
    )
