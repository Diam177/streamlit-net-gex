
# netgex_chart.py
# Drop-in module for rendering the Net GEX per-strike bar chart in Streamlit.
# Usage in streamlit_app.py:
#   from netgex_chart import render_net_gex_bar_chart
#   ...
#   render_net_gex_bar_chart(final_df, S, ticker)

from typing import Optional
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_net_gex_bar_chart(final_df: pd.DataFrame, spot_price: float, ticker: str, title: Optional[str] = "Net GEX по страйкам") -> None:
    """
    Renders a bar chart of Net GEX by strike.
    - final_df must contain at least columns: 'strike' and 'NetGEX' (case-insensitive).
    - spot_price is the current price of underlying (S).
    - ticker is displayed as a small label in the top-left corner.
    Visuals match the provided screenshot: red bars for negative Net GEX, blue for positive,
    orange vertical line at the spot price, and tick labels for all strikes.
    """
    if final_df is None or len(final_df) == 0:
        st.info("Нет данных для графика Net GEX.")
        return

    # normalize columns
    cols_map = {str(c).lower(): c for c in final_df.columns}
    strike_col = cols_map.get("strike")
    netgex_col = cols_map.get("netgex")

    if not strike_col or not netgex_col:
        st.warning("В таблице не найдены нужные столбцы 'strike' и/или 'NetGEX'.")
        return

    df = final_df.copy()
    df[strike_col] = pd.to_numeric(df[strike_col], errors="coerce")
    df[netgex_col] = pd.to_numeric(df[netgex_col], errors="coerce")
    df = df.dropna(subset=[strike_col, netgex_col]).sort_values(strike_col)

    pos = df[netgex_col] >= 0
    neg = ~pos

    fig = go.Figure()

    # Negative (red) bars
    fig.add_bar(
        x=df.loc[neg, strike_col],
        y=df.loc[neg, netgex_col],
        name="Net GEX",
        marker=dict(color="#ff3b30"),
        hovertemplate="Strike: %{x}<br>Net GEX: %{y:,.0f}<extra></extra>",
        legendgroup="gex",
        showlegend=True,
    )

    # Positive (blue) bars
    fig.add_bar(
        x=df.loc[pos, strike_col],
        y=df.loc[pos, netgex_col],
        name="Net GEX",
        marker=dict(color="#31c7ff"),
        hovertemplate="Strike: %{x}<br>Net GEX: %{y:,.0f}<extra></extra>",
        legendgroup="gex",
        showlegend=False,
    )

    # Vertical line for spot price
    y_abs = float(np.nanmax(np.abs(df[netgex_col]))) if len(df) else 1.0
    y_top = y_abs * 1.2
    y_bottom = -y_abs * 1.2

    fig.add_shape(
        type="line",
        x0=spot_price, x1=spot_price,
        y0=y_bottom, y1=y_top,
        line=dict(color="#f4a306", width=2),
    )
    fig.add_annotation(
        x=spot_price, y=y_top,
        text=f"Price: {spot_price:,.2f}",
        showarrow=False,
        font=dict(size=12, color="#f4a306"),
        yshift=10
    )

    # Ticker label
    fig.add_annotation(
        xref="paper", yref="paper", x=0.005, y=0.98,
        text=ticker, showarrow=False,
        font=dict(size=16)
    )

    fig.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        bargap=0.1,
    )

    # Show all strikes on x-axis
    fig.update_xaxes(
        title_text="Strike",
        tickmode="array",
        tickvals=df[strike_col],
        ticktext=[str(int(s)) if float(s).is_integer() else str(s) for s in df[strike_col]],
    )
    fig.update_yaxes(title_text="Net GEX", range=[y_bottom, y_top])

    if title:
        st.subheader(title)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
