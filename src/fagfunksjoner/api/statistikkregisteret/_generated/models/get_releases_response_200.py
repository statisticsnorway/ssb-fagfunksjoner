from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.release_listing import ReleaseListing


T = TypeVar("T", bound="GetReleasesResponse200")


@_attrs_define
class GetReleasesResponse200:
    """
    Attributes:
        total (int | Unset):
        releases (list[ReleaseListing] | Unset):
    """

    total: int | Unset = UNSET
    releases: list[ReleaseListing] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        total = self.total

        releases: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.releases, Unset):
            releases = []
            for releases_item_data in self.releases:
                releases_item = releases_item_data.to_dict()
                releases.append(releases_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if total is not UNSET:
            field_dict["total"] = total
        if releases is not UNSET:
            field_dict["releases"] = releases

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.release_listing import ReleaseListing  # noqa: PLC0415

        d = dict(src_dict)
        total = d.pop("total", UNSET)

        _releases = d.pop("releases", UNSET)
        releases: list[ReleaseListing] | Unset = UNSET
        if _releases is not UNSET:
            releases = []
            for releases_item_data in _releases:
                releases_item = ReleaseListing.from_dict(releases_item_data)

                releases.append(releases_item)

        get_releases_response_200 = cls(
            total=total,
            releases=releases,
        )

        get_releases_response_200.additional_properties = d
        return get_releases_response_200

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
