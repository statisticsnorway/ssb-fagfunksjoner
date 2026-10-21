from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.frequency import Frequency
    from ..models.release_listing_measuring_period import ReleaseListingMeasuringPeriod
    from ..models.release_listing_statistic import ReleaseListingStatistic
    from ..models.revision import Revision


T = TypeVar("T", bound="ReleaseListing")


@_attrs_define
class ReleaseListing:
    """
    Attributes:
        id (int | Unset):
        publish_time (datetime.datetime | Unset):
        period_to (datetime.date | Unset):
        period_from (datetime.date | Unset):
        approval_status (None | str | Unset):
        statistic (ReleaseListingStatistic | Unset):
        measuring_period (ReleaseListingMeasuringPeriod | Unset):
        frequency (Frequency | Unset):
        revision (Revision | Unset): Valid revision codes are:	I (Ingen), B (Beregnede), E (Endelige), F (Foreløpige), R
            (Reviderte), IG (Integrert)
    """

    id: int | Unset = UNSET
    publish_time: datetime.datetime | Unset = UNSET
    period_to: datetime.date | Unset = UNSET
    period_from: datetime.date | Unset = UNSET
    approval_status: None | str | Unset = UNSET
    statistic: ReleaseListingStatistic | Unset = UNSET
    measuring_period: ReleaseListingMeasuringPeriod | Unset = UNSET
    frequency: Frequency | Unset = UNSET
    revision: Revision | Unset = UNSET
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

        frequency: dict[str, Any] | Unset = UNSET
        if not isinstance(self.frequency, Unset):
            frequency = self.frequency.to_dict()

        revision: dict[str, Any] | Unset = UNSET
        if not isinstance(self.revision, Unset):
            revision = self.revision.to_dict()

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
        if frequency is not UNSET:
            field_dict["frequency"] = frequency
        if revision is not UNSET:
            field_dict["revision"] = revision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.frequency import Frequency  # noqa: PLC0415
        from ..models.release_listing_measuring_period import (  # noqa: PLC0415
            ReleaseListingMeasuringPeriod,
        )
        from ..models.release_listing_statistic import (  # noqa: PLC0415
            ReleaseListingStatistic,
        )
        from ..models.revision import Revision  # noqa: PLC0415

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
        statistic: ReleaseListingStatistic | Unset
        if isinstance(_statistic, Unset):
            statistic = UNSET
        else:
            statistic = ReleaseListingStatistic.from_dict(_statistic)

        _measuring_period = d.pop("measuring_period", UNSET)
        measuring_period: ReleaseListingMeasuringPeriod | Unset
        if isinstance(_measuring_period, Unset):
            measuring_period = UNSET
        else:
            measuring_period = ReleaseListingMeasuringPeriod.from_dict(
                _measuring_period
            )

        _frequency = d.pop("frequency", UNSET)
        frequency: Frequency | Unset
        if isinstance(_frequency, Unset):
            frequency = UNSET
        else:
            frequency = Frequency.from_dict(_frequency)

        _revision = d.pop("revision", UNSET)
        revision: Revision | Unset
        if isinstance(_revision, Unset):
            revision = UNSET
        else:
            revision = Revision.from_dict(_revision)

        release_listing = cls(
            id=id,
            publish_time=publish_time,
            period_to=period_to,
            period_from=period_from,
            approval_status=approval_status,
            statistic=statistic,
            measuring_period=measuring_period,
            frequency=frequency,
            revision=revision,
        )

        release_listing.additional_properties = d
        return release_listing

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
