"""This module is automatically generated from the JSON schemas available at:

    https://github.com/statisticsnorway/arkitektur-informasjonsmodeller/tree/main/process-data

It contains pydantic models generated using the `pydantic` tool `datamodel-code-generator`
and the following command::

    poetry run datamodel-codegen --output-model-type pydantic_v2.BaseModel
    --use-standard-collections --use-union-operator
    --input quality-control-description-json-schema.json
    --output quality_control_description.py

Minor modifications were made to the generated code after creation.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class QualityControlType(Enum):
    """Quality control type."""

    H = "H"
    """Hard quality control."""

    S = "S"
    """Soft quality control."""

    I = "I"  # noqa:E741
    """Informative quality control."""


class Variable(BaseModel):
    """A variable included in the quality control."""

    variable_description: str | None = None


class QualityControl(BaseModel):
    """Pydantic model for description of quality controls used in a statistical production."""

    quality_control_id: str = Field(..., description="A unique quality control ID")
    quality_control_description: str = Field(
        ..., description="Quality control description"
    )
    quality_control_type: QualityControlType = Field(
        ..., description="Quality control type: hard (M), soft (S), informative (I)"
    )
    variables: list[Variable] = Field(
        ...,
        description="A description of which variables must be included in the quality control.",
    )
