from fagfunksjoner import SsbFormat
from fagfunksjoner.formats import store_format


frmt1 = dict(
    zip(
        [f"key{i}" for i in range(1, 6)],
        [f"value{j}" for j in range(1, 6)],
        strict=False,
    )
)
frmt2 = dict(
    zip(
        [f"{i}" for i in range(1, 6)],
        [f"category{j}" for j in range(1, 6)],
        strict=False,
    )
)

frmt1 = SsbFormat(frmt1)

name = "test_format_1"
path = "/home/onyxia/work/ssb-fagfunksjoner/demos/test_data/"
frmt1.store(path + name, force=True)

store_format(frmt2, path + "test_format_2")
