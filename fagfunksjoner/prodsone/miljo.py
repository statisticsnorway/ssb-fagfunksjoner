import os


def sjekk_miljo() -> str:
    if os.uname()[1].startswith("sl-"):
        miljo = "PROD"
    elif "dapla" in str(dict(os.environ).values()):
        miljo = "DAPLA"
    else:
        raise OSError("Ikke i prodsonen, eller på Dapla?")
    return miljo


def linux_forkortelser(insert_environ: bool = False) -> dict:
    stm = {}
    with open("/etc/profile.d/stamme_variabel", "r") as stam_var:
        for line in stam_var:
            line = line.strip()
            if line.startswith("export") and "=" in line:
                line_parts = line.replace("export ", "").split("=")
                if len(line_parts) != 2: raise ValueError("For mange likhetstegn?")
                stm[line_parts[0]] = line_parts[1]
                if insert_environ:
                    os.environ[[line_parts[0]]] = line_parts[1]
    return stm