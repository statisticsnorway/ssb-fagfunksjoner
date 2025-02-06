import json
from datetime import datetime

import pytest
from pydantic import ValidationError

from fagfunksjoner.log.process_data import ProcessData
from fagfunksjoner.log.quality_control_description import (
    QualityControl,
    QualityControlType,
    Variable,
)
from fagfunksjoner.log.quality_control_result import (
    QualityControlResult,
    QualityControlResults,
)


def test_process_data_serialization() -> None:
    current_datetime = datetime.now().astimezone()
    process_data = ProcessData(
        statistics_name="metstat",
        data_target="gs://ssb-tip-tutorials-data-produkt-prod/metstat/inndata/frost/weather_stations_v1.parquet",
        data_period="2025-01-21T08:45:45+0100",
        unit_id="komm_nr",
        change_event="A",
        change_event_reason="OTHER_SOURCE",
        change_datetime=current_datetime,
    )
    process_dict = json.loads(process_data.model_dump_json())
    transformed_process_data = ProcessData.model_validate(process_dict)
    assert transformed_process_data == process_data


def test_process_data_validation() -> None:
    invalid_data = {
        "statistics_name": "metstat",
        "data_target": "gs://ssb-tip-tutorials-data-produkt-prod/metstat/inndata/frost/weather_stations_v1.parquet",
        "data_period": "2025-01-21T08:45:45+0100",
        "unit_id": "komm_nr",
        "change_event": "A",
        "change_event_reason": "OTHER_SORCE",
    }
    with pytest.raises(ValidationError):
        ProcessData.model_validate(invalid_data)


def test_quality_control_serialization() -> None:
    quality_control = QualityControl(
        quality_control_id="1",
        quality_control_description="Quality Control Description",
        quality_control_type=QualityControlType.H,
        variables=[Variable(variable_description="komm_nr")],
    )
    quality_control_dict = json.loads(quality_control.model_dump_json())
    transformed_quality_control = QualityControl.model_validate(quality_control_dict)
    assert transformed_quality_control == quality_control


def test_quality_control_validation() -> None:
    invalid_data = {
        "quality_control_id": "1",
        "quality_control_description": "Quality Control Description",
        "quality_control_type": "H",
    }
    with pytest.raises(ValidationError):
        QualityControl.model_validate(invalid_data)


def test_quality_control_result_serialization() -> None:
    quality_control_result = QualityControlResult(
        statistics_name="metstat",
        quality_control_id="1",
        data_location=[
            "gs://ssb-tip-tutorials-data-produkt-prod/metstat/inndata/frost/weather_stations_v1.parquet"
        ],
        data_period="2025-01-21T08:45:45+0100",
        quality_control_datetime="2025-01-21T08:45:45+0100",
        quality_control_results=QualityControlResults.field_1,
    )
    qc_result_dict = json.loads(quality_control_result.model_dump_json())
    transformed_qc_result = QualityControlResult.model_validate(qc_result_dict)
    assert transformed_qc_result == quality_control_result


def test_quality_control_result_validation() -> None:
    invalid_data = {
        "statistics_name": "metstat",
        "quality_control_id": "1",
        "data_location": [
            "gs://ssb-tip-tutorials-data-produkt-prod/metstat/inndata/frost/weather_stations_v1.parquet"
        ],
        "data_period": "2025-01-21T08:45:45+0100",
        "quality_control_datetime": "2025-01-32T08:45:45+0100",
        "quality_control_results": "1",
    }
    with pytest.raises(ValidationError):
        QualityControl.model_validate(invalid_data)
