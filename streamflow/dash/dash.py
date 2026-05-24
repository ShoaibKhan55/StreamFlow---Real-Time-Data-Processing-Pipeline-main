"""
StreamFlow - Real-Time Dashboard
Visualizes cryptocurrency data in real-time
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(
    page_title="StreamFlow Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .big-font {
        font-size:30px !important;
        font-weight: bold;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_database_engine():
    """Create and cache database connection"""
    return create_engine('postgresql://admin:admin123@localhost:5432/streamflow_db')


def fetch_latest_data(symbol, limit=100):
    """Fetch latest data for a symbol"""
    engine = get_database_engine()
    query = text("""
        SELECT 
            symbol,
            price,
            quantity,
            timestamp,
            moving_avg_5,
            moving_avg_20,
            price_change_1m
        FROM crypto_data
        WHERE symbol = :symbol
        ORDER BY timestamp DESC
        LIMIT :limit
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'symbol': symbol, 'limit': limit})
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


def fetch_all_symbols():
    """Fetch list of available symbols"""
    engine = get_database_engine()
    query = text("""
        SELECT DISTINCT symbol
        FROM crypto_data
        ORDER BY symbol
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            symbols = [row[0] for row in result]
        return symbols
    except Exception as e:
        st.error(f"Error fetching symbols: {e}")
        return []


def create_price_chart(df, symbol):
    """Create interactive price chart with moving averages"""
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=(f'{symbol} Price & Moving Averages', 'Trading Volume'),
        row_heights=[0.7, 0.3]
    )
    
    df = df.sort_values('timestamp')
    
    # Price line
    fig.add_trace(
        go.Scatter(
            x=df['timestamp'],
            y=df['price'],
            mode='lines',
            name='Price',
            line=dict(color='#2E86DE', width=2),
            hovertemplate='Price: $%{y:,.2f}<br>Time: %{x}<extra></extra>'
        ),
        row=1, col=1
    )
    
    # Moving Average 5
    if 'moving_avg_5' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=df['moving_avg_5'],
                mode='lines',
                name='MA(5)',
                line=dict(color='#EE5A6F', width=2, dash='dash'),
                hovertemplate='MA(5): $%{y:,.2f}<extra></extra>'
            ),
            row=1, col=1
        )
    
    # Moving Average 20
    if 'moving_avg_20' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=df['moving_avg_20'],
                mode='lines',
                name='MA(20)',
                line=dict(color='#F79F1F', width=2, dash='dot'),
                hovertemplate='MA(20): $%{y:,.2f}<extra></extra>'
            ),
            row=1, col=1
        )
    
    # Volume bars
    if 'quantity' in df.columns:
        fig.add_trace(
            go.Bar(
                x=df['timestamp'],
                y=df['quantity'],
                name='Volume',
                marker_color='#A3CB38',
                hovertemplate='Volume: %{y:.4f}<extra></extra>'
            ),
            row=2, col=1
        )
    
    fig.update_layout(
        height=700,
        showlegend=True,
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    fig.update_xaxes(title_text="Time", row=2, col=1)
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    
    return fig


def main():
    """Main dashboard function"""
    
    # Header
    st.markdown('<p class="big-font">📊 StreamFlow Real-Time Dashboard</p>', unsafe_allow_html=True)
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Settings")
        available_symbols = fetch_all_symbols()
        
        if not available_symbols:
            st.warning("⚠️ No data available. Make sure the producer and processor are running.")
            st.stop()
        
        selected_symbol = st.selectbox("Select Cryptocurrency", available_symbols, index=0)
        refresh_rate = st.slider("Refresh Rate (seconds)", min_value=1, max_value=30, value=5, step=1)
        data_points = st.slider("Data Points to Display", min_value=50, max_value=500, value=100, step=50)
        
        st.markdown("---")
        st.markdown("### 📖 About")
        st.info("""
        **StreamFlow** is a real-time data processing pipeline.
        
        - Data: Binance WebSocket
        - Processing: Apache Kafka
        - Storage: PostgreSQL
        - Visualization: Streamlit
        """)
    
    # Auto-refresh every 'refresh_rate' seconds
    st_autorefresh(interval=refresh_rate*1000, limit=None, key="data_refresh")
    
    # Fetch latest data
    df = fetch_latest_data(selected_symbol, limit=data_points)
    
    if df.empty:
        st.warning(f"⚠️ No data available for {selected_symbol}")
        return
    
    latest = df.iloc[0]
    
    # Metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric(label="💰 Current Price", value=f"${latest['price']:,.2f}", delta=f"{latest.get('price_change_1m', 0):+.2f}%")
    col2.metric(label="📊 MA(5)", value=f"${latest.get('moving_avg_5', 0):,.2f}")
    col3.metric(label="📈 MA(20)", value=f"${latest.get('moving_avg_20', 0):,.2f}")
    col4.metric(label="🔄 Total Trades", value=f"{len(df):,}")
    last_update = datetime.now().strftime("%H:%M:%S")
    col5.metric(label="⏰ Last Update", value=last_update)
    
    st.markdown("---")
    
    # Price chart
    fig = create_price_chart(df, selected_symbol)
    st.plotly_chart(fig, use_container_width=True, key=f"{selected_symbol}_{int(datetime.now().timestamp())}")
    
    # Recent trades table
    st.subheader("📋 Recent Trades")
    display_df = df.head(20).copy()
    display_df['timestamp'] = pd.to_datetime(display_df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
    display_df['price'] = display_df['price'].apply(lambda x: f"${x:,.2f}")
    
    if 'moving_avg_5' in display_df.columns:
        display_df['moving_avg_5'] = display_df['moving_avg_5'].apply(lambda x: f"${x:,.2f}")
    if 'moving_avg_20' in display_df.columns:
        display_df['moving_avg_20'] = display_df['moving_avg_20'].apply(lambda x: f"${x:,.2f}")
    if 'quantity' in display_df.columns:
        display_df['quantity'] = display_df['quantity'].apply(lambda x: f"{x:.4f}")
    
    st.dataframe(
        display_df[[col for col in ['symbol', 'price', 'quantity', 'moving_avg_5', 'moving_avg_20', 'timestamp'] if col in display_df.columns]],
        use_container_width=True,
        height=400,
        key=f"table_{selected_symbol}_{int(datetime.now().timestamp())}"
    )


if __name__ == "__main__":
    main()
