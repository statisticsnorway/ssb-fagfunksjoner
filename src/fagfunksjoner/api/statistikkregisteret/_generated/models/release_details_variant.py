from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.frequency import Frequency
    from ..models.revision import Revision


T = TypeVar("T", bound="ReleaseDetailsVariant")


@_attrs_define
class ReleaseDetailsVariant:
    """
    Attributes:
        id (int | Unset):
        frequency (Frequency | Unset):
        revision (Revision | Unset): Valid revision codes are:	I (Ingen), B (Beregnede), E (Endelige), F (Foreløpige), R
            (Reviderte), IG (Integrert)
    """

    id: int | Unset = UNSET
    frequency: Frequency | Unset = UNSET
    revision: Revision | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

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
        if frequency is not UNSET:
            field_dict["frequency"] = frequency
        if revision is not UNSET:
            field_dict["revision"] = revision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.frequency import Frequency  # noqa: PLC0415
        from ..models.revision import Revision  # noqa: PLC0415

        d = dict(src_dict)
        id = d.pop("id", UNSET)

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

        release_details_variant = cls(
            id=id,
            frequency=frequency,
            revision=revision,
        )

        release_details_variant.additional_properties = d
        return release_details_variant

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
