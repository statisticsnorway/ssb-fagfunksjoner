from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.division import Division
    from ..models.region_level import RegionLevel
    from ..models.statistic_details_contacts_item import StatisticDetailsContactsItem
    from ..models.statistic_details_incoming_relations_item import (
        StatisticDetailsIncomingRelationsItem,
    )
    from ..models.statistic_details_relation import StatisticDetailsRelation
    from ..models.statistic_status import StatisticStatus
    from ..models.variant import Variant


T = TypeVar("T", bound="StatisticDetails")


@_attrs_define
class StatisticDetails:
    """
    Attributes:
        shortname (str | Unset):
        main_language (str | Unset): Main language will be either 'nn' or 'nb' for statistics
        status (StatisticStatus | Unset):
        name (str | Unset):
        name_en (str | Unset):
        approval_status (str | Unset):  Is one of 'GODKJENT', 'AVVIST' or 'FORSLAG'
        version (int | Unset):
        division (Division | Unset):
        first_released_at (datetime.date | None | Unset):
        yearly_reporting (bool | Unset):
        previous_topic_codes (None | str | Unset):
        relation (StatisticDetailsRelation | Unset):
        incoming_relations (list[StatisticDetailsIncomingRelationsItem] | Unset): Statistics that reference this
            statistic as their continuation.
        updated_at (datetime.datetime | Unset):
        comment (str | Unset):
        created_at (datetime.datetime | Unset):
        variants (list[Variant] | Unset):
        contacts (list[StatisticDetailsContactsItem] | Unset):
        statistic_region_levels (list[RegionLevel] | Unset):
    """

    shortname: str | Unset = UNSET
    main_language: str | Unset = UNSET
    status: StatisticStatus | Unset = UNSET
    name: str | Unset = UNSET
    name_en: str | Unset = UNSET
    approval_status: str | Unset = UNSET
    version: int | Unset = UNSET
    division: Division | Unset = UNSET
    first_released_at: datetime.date | None | Unset = UNSET
    yearly_reporting: bool | Unset = UNSET
    previous_topic_codes: None | str | Unset = UNSET
    relation: StatisticDetailsRelation | Unset = UNSET
    incoming_relations: list[StatisticDetailsIncomingRelationsItem] | Unset = UNSET
    updated_at: datetime.datetime | Unset = UNSET
    comment: str | Unset = UNSET
    created_at: datetime.datetime | Unset = UNSET
    variants: list[Variant] | Unset = UNSET
    contacts: list[StatisticDetailsContactsItem] | Unset = UNSET
    statistic_region_levels: list[RegionLevel] | Unset = UNSET
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

        version = self.version

        division: dict[str, Any] | Unset = UNSET
        if not isinstance(self.division, Unset):
            division = self.division.to_dict()

        first_released_at: None | str | Unset
        if isinstance(self.first_released_at, Unset):
            first_released_at = UNSET
        elif isinstance(self.first_released_at, datetime.date):
            first_released_at = self.first_released_at.isoformat()
        else:
            first_released_at = self.first_released_at

        yearly_reporting = self.yearly_reporting

        previous_topic_codes: None | str | Unset
        if isinstance(self.previous_topic_codes, Unset):
            previous_topic_codes = UNSET
        else:
            previous_topic_codes = self.previous_topic_codes

        relation: dict[str, Any] | Unset = UNSET
        if not isinstance(self.relation, Unset):
            relation = self.relation.to_dict()

        incoming_relations: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.incoming_relations, Unset):
            incoming_relations = []
            for incoming_relations_item_data in self.incoming_relations:
                incoming_relations_item = incoming_relations_item_data.to_dict()
                incoming_relations.append(incoming_relations_item)

        updated_at: str | Unset = UNSET
        if not isinstance(self.updated_at, Unset):
            updated_at = self.updated_at.isoformat()

        comment = self.comment

        created_at: str | Unset = UNSET
        if not isinstance(self.created_at, Unset):
            created_at = self.created_at.isoformat()

        variants: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.variants, Unset):
            variants = []
            for variants_item_data in self.variants:
                variants_item = variants_item_data.to_dict()
                variants.append(variants_item)

        contacts: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.contacts, Unset):
            contacts = []
            for contacts_item_data in self.contacts:
                contacts_item = contacts_item_data.to_dict()
                contacts.append(contacts_item)

        statistic_region_levels: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.statistic_region_levels, Unset):
            statistic_region_levels = []
            for statistic_region_levels_item_data in self.statistic_region_levels:
                statistic_region_levels_item = (
                    statistic_region_levels_item_data.to_dict()
                )
                statistic_region_levels.append(statistic_region_levels_item)

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
        if version is not UNSET:
            field_dict["version"] = version
        if division is not UNSET:
            field_dict["division"] = division
        if first_released_at is not UNSET:
            field_dict["first_released_at"] = first_released_at
        if yearly_reporting is not UNSET:
            field_dict["yearly_reporting"] = yearly_reporting
        if previous_topic_codes is not UNSET:
            field_dict["previous_topic_codes"] = previous_topic_codes
        if relation is not UNSET:
            field_dict["relation"] = relation
        if incoming_relations is not UNSET:
            field_dict["incoming_relations"] = incoming_relations
        if updated_at is not UNSET:
            field_dict["updated_at"] = updated_at
        if comment is not UNSET:
            field_dict["comment"] = comment
        if created_at is not UNSET:
            field_dict["created_at"] = created_at
        if variants is not UNSET:
            field_dict["variants"] = variants
        if contacts is not UNSET:
            field_dict["contacts"] = contacts
        if statistic_region_levels is not UNSET:
            field_dict["statistic_region_levels"] = statistic_region_levels

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.division import Division  # noqa: PLC0415
        from ..models.region_level import RegionLevel  # noqa: PLC0415
        from ..models.statistic_details_contacts_item import (  # noqa: PLC0415
            StatisticDetailsContactsItem,
        )
        from ..models.statistic_details_incoming_relations_item import (  # noqa: PLC0415
            StatisticDetailsIncomingRelationsItem,
        )
        from ..models.statistic_details_relation import (  # noqa: PLC0415
            StatisticDetailsRelation,
        )
        from ..models.statistic_status import StatisticStatus  # noqa: PLC0415
        from ..models.variant import Variant  # noqa: PLC0415

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

        version = d.pop("version", UNSET)

        _division = d.pop("division", UNSET)
        division: Division | Unset
        if isinstance(_division, Unset):
            division = UNSET
        else:
            division = Division.from_dict(_division)

        def _parse_first_released_at(data: object) -> datetime.date | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                first_released_at_type_0 = datetime.date.fromisoformat(
                    data.split("T", 1)[0]
                )

                return first_released_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.date | None | Unset, data)

        first_released_at = _parse_first_released_at(d.pop("first_released_at", UNSET))

        yearly_reporting = d.pop("yearly_reporting", UNSET)

        def _parse_previous_topic_codes(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        previous_topic_codes = _parse_previous_topic_codes(
            d.pop("previous_topic_codes", UNSET)
        )

        _relation = d.pop("relation", UNSET)
        relation: StatisticDetailsRelation | Unset
        if isinstance(_relation, Unset):
            relation = UNSET
        else:
            relation = StatisticDetailsRelation.from_dict(_relation)

        _incoming_relations = d.pop("incoming_relations", UNSET)
        incoming_relations: list[StatisticDetailsIncomingRelationsItem] | Unset = UNSET
        if _incoming_relations is not UNSET:
            incoming_relations = []
            for incoming_relations_item_data in _incoming_relations:
                incoming_relations_item = (
                    StatisticDetailsIncomingRelationsItem.from_dict(
                        incoming_relations_item_data
                    )
                )

                incoming_relations.append(incoming_relations_item)

        _updated_at = d.pop("updated_at", UNSET)
        updated_at: datetime.datetime | Unset
        if isinstance(_updated_at, Unset):
            updated_at = UNSET
        else:
            updated_at = datetime.datetime.fromisoformat(
                _updated_at.replace("Z", "+00:00")
            )

        comment = d.pop("comment", UNSET)

        _created_at = d.pop("created_at", UNSET)
        created_at: datetime.datetime | Unset
        if isinstance(_created_at, Unset):
            created_at = UNSET
        else:
            created_at = datetime.datetime.fromisoformat(
                _created_at.replace("Z", "+00:00")
            )

        _variants = d.pop("variants", UNSET)
        variants: list[Variant] | Unset = UNSET
        if _variants is not UNSET:
            variants = []
            for variants_item_data in _variants:
                variants_item = Variant.from_dict(variants_item_data)

                variants.append(variants_item)

        _contacts = d.pop("contacts", UNSET)
        contacts: list[StatisticDetailsContactsItem] | Unset = UNSET
        if _contacts is not UNSET:
            contacts = []
            for contacts_item_data in _contacts:
                contacts_item = StatisticDetailsContactsItem.from_dict(
                    contacts_item_data
                )

                contacts.append(contacts_item)

        _statistic_region_levels = d.pop("statistic_region_levels", UNSET)
        statistic_region_levels: list[RegionLevel] | Unset = UNSET
        if _statistic_region_levels is not UNSET:
            statistic_region_levels = []
            for statistic_region_levels_item_data in _statistic_region_levels:
                statistic_region_levels_item = RegionLevel.from_dict(
                    statistic_region_levels_item_data
                )

                statistic_region_levels.append(statistic_region_levels_item)

        statistic_details = cls(
            shortname=shortname,
            main_language=main_language,
            status=status,
            name=name,
            name_en=name_en,
            approval_status=approval_status,
            version=version,
            division=division,
            first_released_at=first_released_at,
            yearly_reporting=yearly_reporting,
            previous_topic_codes=previous_topic_codes,
            relation=relation,
            incoming_relations=incoming_relations,
            updated_at=updated_at,
            comment=comment,
            created_at=created_at,
            variants=variants,
            contacts=contacts,
            statistic_region_levels=statistic_region_levels,
        )

        statistic_details.additional_properties = d
        return statistic_details

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
