from convertbng.util import convert_bng, convert_lonlat
from typing import List, Union


def convert_bng_to_latlon(
        eastings: Union[float, List[float]],
        northings: Union[float, List[float]],
        **kwargs
):
    if not isinstance(eastings, list):
        eastings = [eastings]

    if not isinstance(northings, list):
        northings = [northings]

    eastings = [float(e) for e in eastings]
    northings = [float(n) for n in northings]
    return convert_lonlat(eastings, northings)


def convert_lonlat_to_bng(
        longitudes: Union[float, List[float]],
        latitudes: Union[float, List[float]],
        **kwargs
):
    if not isinstance(longitudes, list):
        longitudes = [longitudes]

    if not isinstance(latitudes, list):
        latitudes = [latitudes]

    longitudes = [float(x) for x in longitudes]
    latitudes = [float(y) for y in latitudes]
    return convert_bng(longitudes, latitudes)
