import os

def sjekk_miljo():
    if os.uname()[1].startswith("sl-"):
        miljo = "PROD"
    elif "dapla" in str(dict(os.environ).values()):
        miljo = "DAPLA"
    else:
        raise OSError("Ikke i prodsonen, eller på Dapla?")
    return miljo
