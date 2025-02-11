import pandas as pd
from klass import KlassClassification, KlassCorrespondence
from requests.exceptions import HTTPError


def kostra_kommunekorr(year: str) -> pd.DataFrame:
    """Fetches and compiles data on correspondences between municipalities and related classifications for a given year.

    The function retrieves the following:
      - Municipality classification (KLASS 131) and manually adds Longyearbyen.
      - The correspondence between municipality (KLASS 131) and KOSTRA group (KLASS 112). This request is wrapped in a try-except block to catch HTTP 404 errors and raise a descriptive ValueError.
      - The correspondence between municipality (KLASS 131) and county (KLASS 104).

    The retrieved data is merged into a single DataFrame containing information on:
      - Municipality number (komnr) and name (komnavn)
      - County number (fylknr) and name (fylknavn)
      - KOSTRA group number (kostragr) and name (kostragrnavn)
      - Validity start and end dates for both KOSTRA group and county classifications.
      - Additional columns:
          - 'fylknr_eka': county number prefixed with "EKA".
          - 'fylknr_eka_m_tekst': concatenation of 'fylknr_eka' and the county name.
          - 'landet': a static label "EAK Landet".
          - 'landet_u_oslo': a static label "EAKUO Landet uten Oslo" (set to NaN for Oslo, municipality code "0301").

    Args:
        year (str): The year (format "YYYY") for which data should be fetched.

    Returns:
        pd.DataFrame: A DataFrame with the following columns:
            - komnr: Municipality number.
            - komnavn: Municipality name.
            - fylknr: County number.
            - fylknavn: County name.
            - fylknr_eka: County number prefixed with "EKA".
            - fylknr_eka_m_tekst: Combination of fylknr_eka and fylknavn.
            - fylk_validFrom: Start date for county classification validity.
            - fylk_validTo: End date for county classification validity.
            - kostragr: KOSTRA group number.
            - kostragrnavn: KOSTRA group name.
            - kostra_validFrom: Start date for KOSTRA group validity.
            - kostra_validTo: End date for KOSTRA group validity.
            - landet: Static label for the nation.
            - landet_u_oslo: Static label for the nation excluding Oslo.

    Raises:
        ValueError: If the correspondence between municipality and KOSTRA group is not found (e.g., HTTP 404),
                    or if duplicates are detected for municipality numbers after merging the data.

    Example:
        >>> df = kostra_kommunekorr("2025")
        >>> df['verdi'] = 1000
        >>> groups = [
        ...     ['komnr', 'komnavn'],
        ...     ['fylknr', 'fylknavn'],
        ...     ['kostragr', 'kostragrnavn'],
        ...     ['landet_u_oslo'],
        ...     ['landet']
        ... ]
        >>> agg_list = []
        >>> for cols in groups:
        ...     temp = df.groupby(cols)['verdi'].sum().rename('agg_verdi')
        ...     agg_list.append(temp)
        >>> df_agg = pd.DataFrame(pd.concat(agg_list))
    """
    from_date = f"{year}-01-01"
    to_date = f"{year}-12-31"

    kom = (
        KlassClassification(131, language="nb", include_future=False)
        .get_codes(from_date=from_date, to_date=to_date)
        .data[["code", "name"]]
        .rename(columns={"code": "komnr", "name": "komnavn"})
    )

    # Manually add Longyearbyen
    df_longyear = pd.DataFrame({"komnr": ["2111"], "komnavn": ["Longyearbyen"]})
    kom = pd.concat([kom, df_longyear], ignore_index=True)

    # Retrieve the correspondence between municipality and KOSTRA group (KLASS 112)
    try:
        korresp_kostra = KlassCorrespondence(
            source_classification_id="131",
            target_classification_id="112",
            from_date=from_date,
            to_date=to_date,
        )
        kom_kostragr = korresp_kostra.data.rename(
            columns={
                "sourceCode": "komnr",
                "targetCode": "kostragr",
                "targetName": "kostragrnavn",
                "validFrom": "kostra_validFrom",
                "validTo": "kostra_validTo",
            }
        ).drop(columns=["sourceName", "sourceShortName", "targetShortName"])
    except HTTPError as e:
        if e.response.status_code == 404:
            raise ValueError(
                f"KOSTRA group correspondence (131 → 112) for the period {from_date} to {to_date} was not found."
            ) from e
        else:
            raise

    # Retrieve the correspondence between municipality and county (KLASS 104)
    korresp_fyl = KlassCorrespondence(
        source_classification_id="131",
        target_classification_id="104",
        from_date=from_date,
        to_date=to_date,
    )
    kom_fyl = korresp_fyl.data.rename(
        columns={
            "sourceCode": "komnr",
            "targetCode": "fylknr",
            "targetName": "fylknavn",
            "validFrom": "fylk_validFrom",
            "validTo": "fylk_validTo",
        }
    ).drop(columns=["sourceName", "sourceShortName", "targetShortName"])

    # Merge the data
    kom = pd.merge(kom, kom_kostragr, on="komnr", how="left")
    kom = pd.merge(kom, kom_fyl, on="komnr", how="left")

    # Check for duplicate municipality numbers and raise an error if found
    if kom.duplicated("komnr").sum() > 0:
        duplicates = list(kom[kom.duplicated("komnr")]["komnavn"])
        raise ValueError(
            "Duplicates detected for municipality numbers: " + ", ".join(duplicates)
        )

    kom = kom[kom["komnr"] != "9999"].copy()

    # Add extra columns for county data and national categorization
    kom["fylknr_eka"] = "EKA" + kom["fylknr"].str[:2]
    kom["fylknr_eka_m_tekst"] = kom["fylknr_eka"] + " " + kom["fylknavn"]
    kom["landet"] = "EAK Landet"
    kom["landet_u_oslo"] = "EAKUO Landet uten Oslo"
    kom.loc[kom["komnr"] == "0301", "landet_u_oslo"] = pd.NA

    return kom[
        [
            "komnr",
            "komnavn",
            "fylknr",
            "fylknavn",
            "fylknr_eka",
            "fylknr_eka_m_tekst",
            "fylk_validFrom",
            "fylk_validTo",
            "kostragr",
            "kostragrnavn",
            "kostra_validFrom",
            "kostra_validTo",
            "landet",
            "landet_u_oslo",
        ]
    ]
