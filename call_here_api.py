# call_here_api.py
import os
import _snowflake
import requests
from typing import Tuple, Dict, List, Union
from flexpolyline import decode


secret = _snowflake.get_generic_secret_string("here_api_key")
os.environ["HERE_API_KEY"] = secret

def call_geocoding_here_api(address: str) -> Dict:
    params = {
        "q":      address,
        "apiKey": secret,
    }
    resp = requests.get(
        "https://geocode.search.hereapi.com/v1/geocode",
        params=params,
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def call_routing_here_api(
    origin: Tuple[float, float],
    destination: Tuple[float, float],
) -> Dict:
    """
    Call HERE Routing v8 and return the JSON.
    """
    params = {
        "transportMode": "car",
        "origin":        f"{origin[0]},{origin[1]}",
        "destination":   f"{destination[0]},{destination[1]}",
        "return":        "polyline",
        "apikey":        secret,
    }
    # debug print to Streamlit
    import streamlit as st
    st.write(f"🔍 Debug — routing v8 params: {params}")

    resp = requests.get(
        "https://router.hereapi.com/v8/routes",
        params=params,
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def decode_polyline(data: Union[str, Dict]) -> List[Tuple[float, float]]:
    """
    Decode either:
      - a HERE JSON response (dict) by recursing into routes→sections→polyline
      - or a flexpolyline string by calling the imported decode()
    """
    if isinstance(data, dict):
        coords: List[Tuple[float, float]] = []
        for route in data.get("routes", []):
            for section in route.get("sections", []):
                poly = section.get("polyline")
                if poly:
                    coords.extend(decode_polyline(poly))
        return coords

    # Otherwise it's the flexpolyline‐encoded string
    return decode(data)




# call_here_api.py (just replace your old display_map with this)

# in call_here_api.py

def display_map(coords: List[Tuple[float, float]]):
    import pandas as pd
    import streamlit as st
    import pydeck as pdk

    if not coords:
        st.write("No route to display.")
        return

    # Turn [(lat, lon), …] → [[ [lon, lat], … ]]
    path = [[[lon, lat] for lat, lon in coords]]
    df = pd.DataFrame({"path": path})

    # Draw the route in bright red
    layer = pdk.Layer(
        "PathLayer",
        data=df,
        pickable=False,
        get_path="path",
        get_width=10,
        get_color=[255, 0, 0],      # bright red
        get_dash_array=[0, 0],      # [dashLength, gapLength] of 0 → solid
        width_min_pixels=2,         # minimum pixel width
        width_max_pixels=10         # maximum pixel width
    )

    # Center on the first point
    start_lat, start_lon = coords[0]
    view_state = pdk.ViewState(
        latitude=start_lat,
        longitude=start_lon,
        zoom=11
    )

    # Use a light basemap style
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style="mapbox://styles/mapbox/light-v9"
    )

    st.pydeck_chart(deck)
