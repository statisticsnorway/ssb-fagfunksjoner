from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.statistic_listing import StatisticListing


T = TypeVar("T", bound="GetStatisticsResponse200")


@_attrs_define
class GetStatisticsResponse200:
    """
    Attributes:
        total (int | Unset):
        statistics (list[StatisticListing] | Unset):
    """

    total: int | Unset = UNSET
    statistics: list[StatisticListing] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        total = self.total

        statistics: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.statistics, Unset):
            statistics = []
            for statistics_item_data in self.statistics:
                statistics_item = statistics_item_data.to_dict()
                statistics.append(statistics_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if total is not UNSET:
            field_dict["total"] = total
        if statistics is not UNSET:
            field_dict["statistics"] = statistics

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.statistic_listing import StatisticListing  # noqa: PLC0415

        d = dict(src_dict)
        total = d.pop("total", UNSET)

        _statistics = d.pop("statistics", UNSET)
        statistics: list[StatisticListing] | Unset = UNSET
        if _statistics is not UNSET:
            statistics = []
            for statistics_item_data in _statistics:
                statistics_item = StatisticListing.from_dict(statistics_item_data)

                statistics.append(statistics_item)

        get_statistics_response_200 = cls(
            total=total,
            statistics=statistics,
        )

        get_statistics_response_200.additional_properties = d
        return get_statistics_response_200

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
