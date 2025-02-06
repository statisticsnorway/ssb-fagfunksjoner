"""This module is automatically generated from the JSON schemas available at:

    https://github.com/statisticsnorway/arkitektur-informasjonsmodeller/tree/main/process-data

It contains pydantic models generated using the `pydantic` tool `datamodel-code-generator`
and the following command::

    poetry run datamodel-codegen --output-model-type pydantic_v2.BaseModel
    --use-standard-collections --use-union-operator
    --input process-data-json-schema.json --output process_data.py

Minor modifications were made to the generated code after creation.

Examples:
    The example below shows how to log process data.

    >>> from fagfunksjoner.log.process_data import ProcessData
    >>> from fagfunksjoner.log.statlogger import LoggerType, StatLogger
    >>> from datetime import datetime
    >>> import logging
    >>> import json
    >>>
    >>> root_logger = StatLogger(loggers=[LoggerType.JSONL_EXTRA_ONLY])
    >>> logger = logging.getLogger(__name__)
    >>>
    >>> process_data = ProcessData(
    >>>     statistics_name="metstat",
    >>>     data_target="gs://ssb-tip-tutorials-data-produkt-prod/metstat/inndata/frost/weather_stations_v1.parquet",
    >>>     data_period="2025-01-21T08:45:45+0100",
    >>>     unit_id="komm_nr",
    >>>     change_event="A",
    >>>     change_event_reason="OTHER_SOURCE",
    >>>     change_datetime=datetime.now().astimezone(),
    >>> )
    >>> process_data_dict = json.loads(process_data.model_dump_json())
    >>> logger.info("Log processdata", extra={"data": process_data_dict})
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class UnitIdItem(BaseModel):
    """UnitIdItem is used when unit_id consists of composite keys."""

    unit_id_variable: str | None = None
    unit_id_value: str | None = None


class ChangeEvent(Enum):
    """How the event was triggered."""

    A = "A"
    """Automatically changed."""

    M = "M"
    """Manually changed."""

    MNC = "MNC"
    """Manually approved with no change."""

    NOT = "NOT"
    """Not reviewed."""


class ChangeEventReason(Enum):
    """Reason for change or approval."""

    OTHER_SOURCE = "OTHER_SOURCE"
    """Other source."""

    REVIEW = "REVIEW"
    """Statistical review."""

    OWNER = "OWNER"
    """Information from the data provider/registry owner."""

    MARGINAL_UNIT = "MARGINAL_UNIT"
    """Small/marginal unit."""

    OTHER = "OTHER"
    """Other reason."""


class DataChangeType(Enum):
    """Data change type."""

    NEW = "NEW"
    """Created new unit/row."""

    UPD = "UPD"
    """Updated value."""

    DEL = "DEL"
    """Deleted unit/row."""


class OldValueItem(BaseModel):
    """Previous value if value updated (data_change_type = UPD)."""

    variable_name: str | None = None
    value: str | None = None


class NewValueItem(BaseModel):
    """The new value if value updated (data_change_type = UPD)."""

    variable_name: str | None = None
    value: str | None = None


class ProcessData(BaseModel):
    """Pydantic model for process data in a statistical production process."""

    statistics_name: str = Field(
        ..., description="Statistics shortname or statistics product name"
    )
    data_source: list[str] | None = Field(
        None,
        description="Reference or filepath to one or more input dataset (start data) used as data source before changing data (if applicable)",
    )
    data_target: str = Field(
        ...,
        description="Target dataset filepath (eg. GCS-path to a parquet file) or other dataset reference (eg. ref. to a CloudSQL database table).",
    )
    data_period: str = Field(
        ..., description="Data period for changed data - eg. year, date, date-time, ..."
    )
    unit_id: str | list[UnitIdItem] = Field(..., description="Unit identification.")
    variable_name: str | None = Field(
        None,
        description="The variable name (or elment-path) that contains data changes.",
    )
    change_event: ChangeEvent = Field(
        ...,
        description="How the event was triggered: Automatically changed (A), Manually changed (M), Manually approved with no change (MNC), Not reviewed (NOT)",
    )
    change_event_reason: ChangeEventReason | None = Field(
        None,
        description="Reason for change or approval: Other source (OTHER_SOURCE), Statistical review (REVIEW), Information from the data provider/registry owner (OWNER), Small/marginal unit (MARGINAL_UNIT), Other reason (OTHER)",
    )
    change_datetime: datetime = Field(
        ..., description="Timestamp (date and time, ISO 8601) of an event or change"
    )
    changed_by: str | None = Field(
        None,
        description="If manually (M): email address of the person who triggered an event; if automatically (A) name of method, function and/or process.",
    )
    data_change_type: DataChangeType | None = Field(
        None,
        description="Data change type: Updated value (UPD), created new unit/row (NEW), or deleted unit/row (DEL)",
    )
    old_value: str | list[OldValueItem] | dict[str, Any] | None = Field(
        None, description="Old value(s)"
    )
    new_value: str | list[NewValueItem] | dict[str, Any] | None = Field(
        None, description="New value(s)"
    )
    change_comment: str | None = Field(None, description="Change comment")
    data_version: str | None = Field(
        None, description="Dataset version if applicable, eg. 1, 2 or 3"
    )
