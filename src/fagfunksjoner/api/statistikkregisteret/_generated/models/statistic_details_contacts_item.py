from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="StatisticDetailsContactsItem")


@_attrs_define
class StatisticDetailsContactsItem:
    """
    Attributes:
        principal_name (str):
        name (str | Unset):
        email (str | Unset):
        phone (str | Unset):
    """

    principal_name: str
    name: str | Unset = UNSET
    email: str | Unset = UNSET
    phone: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        principal_name = self.principal_name

        name = self.name

        email = self.email

        phone = self.phone

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "principalName": principal_name,
            }
        )
        if name is not UNSET:
            field_dict["name"] = name
        if email is not UNSET:
            field_dict["email"] = email
        if phone is not UNSET:
            field_dict["phone"] = phone

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        principal_name = d.pop("principalName")

        name = d.pop("name", UNSET)

        email = d.pop("email", UNSET)

        phone = d.pop("phone", UNSET)

        statistic_details_contacts_item = cls(
            principal_name=principal_name,
            name=name,
            email=email,
            phone=phone,
        )

        statistic_details_contacts_item.additional_properties = d
        return statistic_details_contacts_item

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
