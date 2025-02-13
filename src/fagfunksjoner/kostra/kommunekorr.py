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
      - Municipality number (kom_nr) and name (kom_navn)
      - County number (fylk_nr) and name (fylk_navn)
      - KOSTRA group number (kostra_gr) and name (kostra_gr_navn)
      - Validity start and end dates for both KOSTRA group and county classifications.
      - Additional columns:
          - 'fylk_nr_eka': county number prefixed with "EKA".
          - 'fylk_nr_eka_m_tekst': concatenation of 'fylk_nr_eka' and the county name.
          - 'landet': a static label "EAK Landet".
          - 'landet_u_oslo': a static label "EAKUO Landet uten Oslo" (set to NaN for Oslo, municipality code "0301").

    Args:
        year (str): The year (format "YYYY") for which data should be fetched.

    Returns:
        pd.DataFrame: A DataFrame with the following columns:
            - kom_nr: Municipality number.
            - kom_navn: Municipality name.
            - fylk_nr: County number.
            - fylk_navn: County name.
            - fylk_nr_eka: County number prefixed with "EKA".
            - fylk_nr_eka_m_tekst: Combination of fylk_nr_eka and fylk_navn.
            - fylk_validFrom: Start date for county classification validity.
            - fylk_validTo: End date for county classification validity.
            - kostra_gr: KOSTRA group number.
            - kostra_gr_navn: KOSTRA group name.
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
        ...     ['kom_nr', 'kom_navn'],
        ...     ['fylk_nr', 'fylk_navn'],
        ...     ['kostra_gr', 'kostra_gr_navn'],
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
        KlassClassification("131", language="nb", include_future=False)
        .get_codes(from_date=from_date, to_date=to_date)
        .data[["code", "name"]]
        .rename(columns={"code": "kom_nr", "name": "kom_navn"})
    )

    # Manually add Longyearbyen
    df_longyear = pd.DataFrame({"kom_nr": ["2111"], "kom_navn": ["Longyearbyen"]})
    kom = pd.concat([kom, df_longyear], ignore_index=True)

    # Retrieve the correspondence between municipality and KOSTRA group (KLASS 112)
    try:
        korresp_kostra = KlassCorrespondence(
            source_classification_id="131",
            target_classification_id="112",
            from_date=from_date,
            to_date=to_date,
        )
        kom_kostra_gr = korresp_kostra.data.rename(
            columns={
                "sourceCode": "kom_nr",
                "targetCode": "kostra_gr",
                "targetName": "kostra_gr_navn",
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
            "sourceCode": "kom_nr",
            "targetCode": "fylk_nr",
            "targetName": "fylk_navn",
            "validFrom": "fylk_validFrom",
            "validTo": "fylk_validTo",
        }
    ).drop(columns=["sourceName", "sourceShortName", "targetShortName"])

    # Merge the data
    kom = pd.merge(kom, kom_kostra_gr, on="kom_nr", how="left")
    kom = pd.merge(kom, kom_fyl, on="kom_nr", how="left")

    # Check for duplicate municipality numbers and raise an error if found
    if kom.duplicated("kom_nr").sum() > 0:
        duplicates = list(kom[kom.duplicated("kom_nr")]["kom_navn"])
        raise ValueError(
            "Duplicates detected for municipality numbers: " + ", ".join(duplicates)
        )

    kom = kom[kom["kom_nr"] != "9999"].copy()

    # Add extra columns for county data and national categorization
    kom["fylk_nr_eka"] = "EKA" + kom["fylk_nr"].str[:2]
    kom["fylk_nr_eka_m_tekst"] = kom["fylk_nr_eka"] + " " + kom["fylk_navn"]
    kom["landet"] = "EAK Landet"
    kom["landet_u_oslo"] = "EAKUO Landet uten Oslo"
    kom.loc[kom["kom_nr"] == "0301", "landet_u_oslo"] = pd.NA

    return kom[
        [
            "kom_nr",
            "kom_navn",
            "fylk_nr",
            "fylk_navn",
            "fylk_nr_eka",
            "fylk_nr_eka_m_tekst",
            "fylk_validFrom",
            "fylk_validTo",
            "kostra_gr",
            "kostra_gr_navn",
            "kostra_validFrom",
            "kostra_validTo",
            "landet",
            "landet_u_oslo",
        ]
    ]
