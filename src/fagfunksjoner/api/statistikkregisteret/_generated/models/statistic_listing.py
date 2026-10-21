from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.contact import Contact
    from ..models.division import Division
    from ..models.statistic_status import StatisticStatus


T = TypeVar("T", bound="StatisticListing")


@_attrs_define
class StatisticListing:
    """
    Attributes:
        shortname (str | Unset):
        main_language (str | Unset): Main language will be either 'nn' or 'nb' for statistics
        status (StatisticStatus | Unset):
        name (str | Unset):
        name_en (str | Unset):
        approval_status (str | Unset):  Is one of 'GODKJENT', 'AVVIST' or 'FORSLAG'
        id (int | Unset):
        division (Division | Unset):
        contacts (list[Contact] | Unset):
    """

    shortname: str | Unset = UNSET
    main_language: str | Unset = UNSET
    status: StatisticStatus | Unset = UNSET
    name: str | Unset = UNSET
    name_en: str | Unset = UNSET
    approval_status: str | Unset = UNSET
    id: int | Unset = UNSET
    division: Division | Unset = UNSET
    contacts: list[Contact] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        shortname = self.shortname

        main_language = self.main_language

        status: dict[str, Any] | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.to_dict()

        name = self.name

        name_en = self.name_en

        approval_status = self.approval_status

        id = self.id

        division: dict[str, Any] | Unset = UNSET
        if not isinstance(self.division, Unset):
            division = self.division.to_dict()

        contacts: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.contacts, Unset):
            contacts = []
            for contacts_item_data in self.contacts:
                contacts_item = contacts_item_data.to_dict()
                contacts.append(contacts_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if shortname is not UNSET:
            field_dict["shortname"] = shortname
        if main_language is not UNSET:
            field_dict["main_language"] = main_language
        if status is not UNSET:
            field_dict["status"] = status
        if name is not UNSET:
            field_dict["name"] = name
        if name_en is not UNSET:
            field_dict["name_en"] = name_en
        if approval_status is not UNSET:
            field_dict["approval_status"] = approval_status
        if id is not UNSET:
            field_dict["id"] = id
        if division is not UNSET:
            field_dict["division"] = division
        if contacts is not UNSET:
            field_dict["contacts"] = contacts

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.contact import Contact  # noqa: PLC0415
        from ..models.division import Division  # noqa: PLC0415
        from ..models.statistic_status import StatisticStatus  # noqa: PLC0415

        d = dict(src_dict)
        shortname = d.pop("shortname", UNSET)

        main_language = d.pop("main_language", UNSET)

        _status = d.pop("status", UNSET)
        status: StatisticStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = StatisticStatus.from_dict(_status)

        name = d.pop("name", UNSET)

        name_en = d.pop("name_en", UNSET)

        approval_status = d.pop("approval_status", UNSET)

        id = d.pop("id", UNSET)

        _division = d.pop("division", UNSET)
        division: Division | Unset
        if isinstance(_division, Unset):
            division = UNSET
        else:
            division = Division.from_dict(_division)

        _contacts = d.pop("contacts", UNSET)
        contacts: list[Contact] | Unset = UNSET
        if _contacts is not UNSET:
            contacts = []
            for contacts_item_data in _contacts:
                contacts_item = Contact.from_dict(contacts_item_data)

                contacts.append(contacts_item)

        statistic_listing = cls(
            shortname=shortname,
            main_language=main_language,
            status=status,
            name=name,
            name_en=name_en,
            approval_status=approval_status,
            id=id,
            division=division,
            contacts=contacts,
        )

        statistic_listing.additional_properties = d
        return statistic_listing

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
