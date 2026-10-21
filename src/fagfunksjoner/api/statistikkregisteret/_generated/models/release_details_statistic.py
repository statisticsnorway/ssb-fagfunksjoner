from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ReleaseDetailsStatistic")


@_attrs_define
class ReleaseDetailsStatistic:
    """
    Attributes:
        id (int | Unset):
        shortname (str | Unset):
        name (str | Unset):
        name_en (str | Unset):
    """

    id: int | Unset = UNSET
    shortname: str | Unset = UNSET
    name: str | Unset = UNSET
    name_en: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        shortname = self.shortname

        name = self.name

        name_en = self.name_en

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if id is not UNSET:
            field_dict["id"] = id
        if shortname is not UNSET:
            field_dict["shortname"] = shortname
        if name is not UNSET:
            field_dict["name"] = name
        if name_en is not UNSET:
            field_dict["name_en"] = name_en

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id", UNSET)

        shortname = d.pop("shortname", UNSET)

        name = d.pop("name", UNSET)

        name_en = d.pop("name_en", UNSET)

        release_details_statistic = cls(
            id=id,
            shortname=shortname,
            name=name,
            name_en=name_en,
        )

        release_details_statistic.additional_properties = d
        return release_details_statistic

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
