from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.frequency import Frequency
    from ..models.level_of_detail import LevelOfDetail
    from ..models.revision import Revision


T = TypeVar("T", bound="Variant")


@_attrs_define
class Variant:
    """
    Attributes:
        id (int | Unset): On update, provide the id of an existing variant to update it, or omit it to create a new
            variant. Ignored/read-only when creating a statistic.
        version (int | Unset):
        updated_at (datetime.datetime | Unset):
        level_of_detail (LevelOfDetail | Unset): Text field to specify variant beyond frequency and revision
        created_at (datetime.datetime | Unset):
        cancelled (bool | Unset):
        frequency (Frequency | Unset):
        revision (Revision | Unset): Valid revision codes are:	I (Ingen), B (Beregnede), E (Endelige), F (Foreløpige), R
            (Reviderte), IG (Integrert)
    """

    id: int | Unset = UNSET
    version: int | Unset = UNSET
    updated_at: datetime.datetime | Unset = UNSET
    level_of_detail: LevelOfDetail | Unset = UNSET
    created_at: datetime.datetime | Unset = UNSET
    cancelled: bool | Unset = UNSET
    frequency: Frequency | Unset = UNSET
    revision: Revision | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        version = self.version

        updated_at: str | Unset = UNSET
        if not isinstance(self.updated_at, Unset):
            updated_at = self.updated_at.isoformat()

        level_of_detail: dict[str, Any] | Unset = UNSET
        if not isinstance(self.level_of_detail, Unset):
            level_of_detail = self.level_of_detail.to_dict()

        created_at: str | Unset = UNSET
        if not isinstance(self.created_at, Unset):
            created_at = self.created_at.isoformat()

        cancelled = self.cancelled

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
        if version is not UNSET:
            field_dict["version"] = version
        if updated_at is not UNSET:
            field_dict["updated_at"] = updated_at
        if level_of_detail is not UNSET:
            field_dict["level_of_detail"] = level_of_detail
        if created_at is not UNSET:
            field_dict["created_at"] = created_at
        if cancelled is not UNSET:
            field_dict["cancelled"] = cancelled
        if frequency is not UNSET:
            field_dict["frequency"] = frequency
        if revision is not UNSET:
            field_dict["revision"] = revision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.frequency import Frequency  # noqa: PLC0415
        from ..models.level_of_detail import LevelOfDetail  # noqa: PLC0415
        from ..models.revision import Revision  # noqa: PLC0415

        d = dict(src_dict)
        id = d.pop("id", UNSET)

        version = d.pop("version", UNSET)

        _updated_at = d.pop("updated_at", UNSET)
        updated_at: datetime.datetime | Unset
        if isinstance(_updated_at, Unset):
            updated_at = UNSET
        else:
            updated_at = datetime.datetime.fromisoformat(
                _updated_at.replace("Z", "+00:00")
            )

        _level_of_detail = d.pop("level_of_detail", UNSET)
        level_of_detail: LevelOfDetail | Unset
        if isinstance(_level_of_detail, Unset):
            level_of_detail = UNSET
        else:
            level_of_detail = LevelOfDetail.from_dict(_level_of_detail)

        _created_at = d.pop("created_at", UNSET)
        created_at: datetime.datetime | Unset
        if isinstance(_created_at, Unset):
            created_at = UNSET
        else:
            created_at = datetime.datetime.fromisoformat(
                _created_at.replace("Z", "+00:00")
            )

        cancelled = d.pop("cancelled", UNSET)

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

        variant = cls(
            id=id,
            version=version,
            updated_at=updated_at,
            level_of_detail=level_of_detail,
            created_at=created_at,
            cancelled=cancelled,
            frequency=frequency,
            revision=revision,
        )

        variant.additional_properties = d
        return variant

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
