import pandas as pd


def read_datfile(path: str, input_setn: str) -> pd.DataFrame:
    # Parse input
    spots = {}
    for row in input_setn.split("\n"):
        if row.strip():
            row = row.replace("@","")
            name = row.strip().split(" ")[1]
            start = row.strip().split(" ")[0]
            width = "".join([c for c in row.strip().split(" ")[-1].split(".")[0] if c.isdigit()])
            spots[name] = (start, width)
    widths = []
    names = []
    i = 1
    for k, v in spots.items():
        name = k
        k = int(v[0])
        v = int(v[1])
        if i == 1 and k != 1:
            widths.append(k-1)
            names.append(f"delete_{i}")
        elif int(k) > i+1:
            widths.append(k-i-1)
            names.append(f"delete_{i}")
        widths.append(int(v))
        names.append(name)
        i = sum(widths)
    # Read the actual file
    df = pd.read_fwf(
        path, widths=widths, names=names, converters={h: str for h in names}
    )
    return df[[col for col in df.columns if not col.startswith("delete_")]]
