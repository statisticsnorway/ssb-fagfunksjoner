import logging
import os
from io import StringIO

TEAM_NAME = os.environ.get("DAPLA_TEAM", "dapla-felles")
DESTINATION_BUCKET = "gs://ssb-{TEAM_NAME}-data-produkt-prod"


# Canonical kolonner
CANON_FNR = "fnr"  # 11 siffer, kan være NA
CANON_SNR = "snr"  # 7 tegn etter pseudo, ellers UUID
CANON_SNR_MRK = "snr_mrk"  # bool[pyarrow]: True = ikke stabil id (UUID-fylt)
CANON_BIRTH = "pers_foedselsdato"  # YYYYMMDD
CANON_GENDER = "pers_kjoenn"  # '1'/'2'
CANON_PERSNAVN = "elevnavn"  # renset navn


# Kolonner som indikerer persondata
PERSON_COLS = {CANON_FNR, CANON_BIRTH, CANON_GENDER}

WHODAT_VARIABLE_MAP = {
    CANON_PERSNAVN: "navn",
    CANON_GENDER: "kjoenn",
    CANON_BIRTH: "foedselsdato",
    "komm_nr": "kommunenummer",
    "fylkesnummer": "fylkesnummer",
}

WORK_COLS = set(WHODAT_VARIABLE_MAP.values())

MAX_WHODAT_SHARE = 0.10
MAX_WHODAT_ROWS = 50_000


# Important that this picks the logger of kildomaten, and not necessarily the logger from fagfunksjoner-package
logger = logging.getLogger()
logger.setLevel(logging.INFO)
report_contents = StringIO()
logger.addHandler(logging.StreamHandler(report_contents))
