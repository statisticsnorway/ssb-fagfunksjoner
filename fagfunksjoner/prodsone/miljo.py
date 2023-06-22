import os


def sjekk_miljo() -> str:
    if "bruker" in os.listdir("/ssb"):
        miljo = "PROD"
    elif "DATA_MAINTENANCE_URL" in os.environ.keys():
        if "dapla" in os.environ["DATA_MAINTENANCE_URL"]:
            miljo = "DAPLA"
        else:
            raise ValueError("You are confusing me with your DATA_MAINTENANCE_URL")
    else:
        raise OSError("Ikke i prodsonen, eller på Dapla?")
    return miljo


def linux_forkortelser(insert_environ: bool = False) -> dict:
    stm = {}
    with open("/etc/profile.d/stamme_variabel") as stam_var:
        for line in stam_var:
            line = line.strip()
            if line.startswith("export") and "=" in line:
                line_parts = line.replace("export ", "").split("=")
                if len(line_parts) != 2:
                    raise ValueError("For mange likhetstegn?")
                stm[line_parts[0]] = line_parts[1]
                if insert_environ:
                    os.environ[[line_parts[0]]] = line_parts[1]
    return stm
