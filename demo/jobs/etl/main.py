import pandas as pd
from helpers import normalize


def run(session, *names):
    # pandas comes from Environment(packages=...) in the sample
    frame = pd.DataFrame({"name": [normalize(n) for n in names]})
    return {"names": frame["name"].tolist(), "pandas": pd.__version__}
