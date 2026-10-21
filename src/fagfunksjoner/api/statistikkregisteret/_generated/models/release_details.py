from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.release_details_measuring_period import ReleaseDetailsMeasuringPeriod
    from ..models.release_details_statistic import ReleaseDetailsStatistic
    from ..models.release_details_variant import ReleaseDetailsVariant


T = TypeVar("T", bound="ReleaseDetails")


@_attrs_define
class ReleaseDetails:
    """
    Attributes:
        id (int | Unset):
        publish_time (datetime.datetime | Unset):
        period_to (datetime.date | Unset):
        period_from (datetime.date | Unset):
        approval_status (None | str | Unset):
        statistic (ReleaseDetailsStatistic | Unset):
        measuring_period (ReleaseDetailsMeasuringPeriod | Unset):
        has_versions (bool | Unset):
        variant (ReleaseDetailsVariant | Unset):
        cancelled (bool | Unset):
        archived (bool | Unset):
        release_date_precision (str | Unset):
    """

    id: int | Unset = UNSET
    publish_time: datetime.datetime | Unset = UNSET
    period_to: datetime.date | Unset = UNSET
    period_from: datetime.date | Unset = UNSET
    approval_status: None | str | Unset = UNSET
    statistic: ReleaseDetailsStatistic | Unset = UNSET
    measuring_period: ReleaseDetailsMeasuringPeriod | Unset = UNSET
    has_versions: bool | Unset = UNSET
    variant: ReleaseDetailsVariant | Unset = UNSET
    cancelled: bool | Unset = UNSET
    archived: bool | Unset = UNSET
    release_date_precision: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        publish_time: str | Unset = UNSET
        if not isinstance(self.publish_time, Unset):
            publish_time = self.publish_time.isoformat()

        period_to: str | Unset = UNSET
        if not isinstance(self.period_to, Unset):
            period_to = self.period_to.isoformat()

        period_from: str | Unset = UNSET
        if not isinstance(self.period_from, Unset):
            period_from = self.period_from.isoformat()

        approval_status: None | str | Unset
        if isinstance(self.approval_status, Unset):
            approval_status = UNSET
        else:
            approval_status = self.approval_status

        statistic: dict[str, Any] | Unset = UNSET
        if not isinstance(self.statistic, Unset):
            statistic = self.statistic.to_dict()

        measuring_period: dict[str, Any] | Unset = UNSET
        if not isinstance(self.measuring_period, Unset):
            measuring_period = self.measuring_period.to_dict()

        has_versions = self.has_versions

        variant: dict[str, Any] | Unset = UNSET
        if not isinstance(self.variant, Unset):
            variant = self.variant.to_dict()

        cancelled = self.cancelled

        archived = self.archived

        release_date_precision = self.release_date_precision

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if id is not UNSET:
            field_dict["id"] = id
        if publish_time is not UNSET:
            field_dict["publish_time"] = publish_time
        if period_to is not UNSET:
            field_dict["period_to"] = period_to
        if period_from is not UNSET:
            field_dict["period_from"] = period_from
        if approval_status is not UNSET:
            field_dict["approval_status"] = approval_status
        if statistic is not UNSET:
            field_dict["statistic"] = statistic
        if measuring_period is not UNSET:
            field_dict["measuring_period"] = measuring_period
        if has_versions is not UNSET:
            field_dict["has_versions"] = has_versions
        if variant is not UNSET:
            field_dict["variant"] = variant
        if cancelled is not UNSET:
            field_dict["cancelled"] = cancelled
        if archived is not UNSET:
            field_dict["archived"] = archived
        if release_date_precision is not UNSET:
            field_dict["release_date_precision"] = release_date_precision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.release_details_measuring_period import (  # noqa: PLC0415
            ReleaseDetailsMeasuringPeriod,
        )
        from ..models.release_details_statistic import (  # noqa: PLC0415
            ReleaseDetailsStatistic,
        )
        from ..models.release_details_variant import (  # noqa: PLC0415
            ReleaseDetailsVariant,
        )

        d = dict(src_dict)
        id = d.pop("id", UNSET)

        _publish_time = d.pop("publish_time", UNSET)
        publish_time: datetime.datetime | Unset
        if isinstance(_publish_time, Unset):
            publish_time = UNSET
        else:
            publish_time = datetime.datetime.fromisoformat(
                _publish_time.replace("Z", "+00:00")
            )

        _period_to = d.pop("period_to", UNSET)
        period_to: datetime.date | Unset
        if isinstance(_period_to, Unset):
            period_to = UNSET
        else:
            period_to = datetime.date.fromisoformat(_period_to.split("T", 1)[0])

        _period_from = d.pop("period_from", UNSET)
        period_from: datetime.date | Unset
        if isinstance(_period_from, Unset):
            period_from = UNSET
        else:
            period_from = datetime.date.fromisoformat(_period_from.split("T", 1)[0])

        def _parse_approval_status(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        approval_status = _parse_approval_status(d.pop("approval_status", UNSET))

        _statistic = d.pop("statistic", UNSET)
        statistic: ReleaseDetailsStatistic | Unset
        if isinstance(_statistic, Unset):
            statistic = UNSET
        else:
            statistic = ReleaseDetailsStatistic.from_dict(_statistic)

        _measuring_period = d.pop("measuring_period", UNSET)
        measuring_period: ReleaseDetailsMeasuringPeriod | Unset
        if isinstance(_measuring_period, Unset):
            measuring_period = UNSET
        else:
            measuring_period = ReleaseDetailsMeasuringPeriod.from_dict(
                _measuring_period
            )

        has_versions = d.pop("has_versions", UNSET)

        _variant = d.pop("variant", UNSET)
        variant: ReleaseDetailsVariant | Unset
        if isinstance(_variant, Unset):
            variant = UNSET
        else:
            variant = ReleaseDetailsVariant.from_dict(_variant)

        cancelled = d.pop("cancelled", UNSET)

        archived = d.pop("archived", UNSET)

        release_date_precision = d.pop("release_date_precision", UNSET)

        release_details = cls(
            id=id,
            publish_time=publish_time,
            period_to=period_to,
            period_from=period_from,
            approval_status=approval_status,
            statistic=statistic,
            measuring_period=measuring_period,
            has_versions=has_versions,
            variant=variant,
            cancelled=cancelled,
            archived=archived,
            release_date_precision=release_date_precision,
        )

        release_details.additional_properties = d
        return release_details

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
