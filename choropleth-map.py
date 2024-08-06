from cassandra.cluster import Cluster
import pandas as pd
import json
import plotly.express as px
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns

def Make_choropleth_map(rows):
    df = pd.DataFrame(list(rows))
    df = df.groupby('district', as_index=False)['price'].mean().reset_index()
    
    # Đọc dữ liệu GeoJSON
    with open("./hochiminh.geojson", encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    # Tạo bản đồ choropleth
    fig = px.choropleth_mapbox(
    df,
    geojson=geojson_data,
    locations='district',
    featureidkey="properties.Ten_Huyen",
    color='price',
    color_continuous_scale="Plasma",  # Thay đổi thang màu sắc
    mapbox_style="carto-positron",
    zoom=9,
    center={"lat": 10.780703293586301, "lon": 106.64531555779595}
    )
    
    # Cập nhật bố cục bản đồ
    fig.update_layout(
        mapbox_accesstoken='pk.eyJ1IjoiY2hpZW4yMTY1IiwiYSI6ImNsemQyazZ4bTBqMmgyaXB2Z2RmbmlpOTMifQ.suFUwsOYdTSGwu__VO1hUw',
        mapbox_style="carto-positron",
        margin={"r":0, "t":0, "l":0, "b":0}
    )
    
    return fig

def Statics_price(df):
    # Tính toán các thống kê cơ bản
    sum_price = df['price'].sum()
    mean_price = df['price'].mean()
    std_price = df['price'].std()
    with st.container():
        st.write("**Tổng Giá Thuê Thu Thập**")
        st.write(f"<div style='border: 1px solid #ddd; padding: 10px; border-radius: 5px; font-size: 20px; color: #4CAF50;'>{sum_price/1000:,.2f} Tỷ VND</div>", unsafe_allow_html=True)
        st.write("**Giá thuê trung bình:**")
        st.write(f"<div style='border: 1px solid #ddd; padding: 10px; border-radius: 5px; font-size: 20px; color: #4CAF50;'>{mean_price:,.2f} Triệu VND</div>", unsafe_allow_html=True)
        st.write("**Độ lệch giá thuê:**")
        st.write(f"<div style='border: 1px solid #ddd; padding: 10px; border-radius: 5px; font-size: 20px; color: #F44336;'>{std_price:,.2f}</div>", unsafe_allow_html=True)

def bar_plot_square_area(df):
    df = df.groupby('district', as_index=False)['area'].mean().reset_index() 
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x='district', y='area', data=df, palette='viridis', ax=ax)
    ax.set_xlabel('District')
    ax.set_ylabel('m2/căn hộ')
    ax.set_title('Biểu đồ trung bình diện tích /căn hộ của mỗi quận/huyện')
    plt.xticks(rotation=45)
    st.pyplot(fig)

def bar_plot_price_m2(df):
    df['price/m2'] = df["price"]/df["area"]*1000000
    df = df.groupby('district', as_index=False)['price/m2'].mean().reset_index()        
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(x='district', y='price/m2', data=df, palette='viridis', ax=ax)
    ax.set_xlabel('District')
    ax.set_ylabel('Giá/m2')
    ax.set_title('Biểu đồ giá/m2 của mỗi quận/huyện')
    plt.xticks(rotation=45)
    st.pyplot(fig)   
def pie_chart(df):
    category_counts = df['category'].value_counts()
    labels = category_counts.index
    sizes = category_counts.values

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.set_title('Tỷ lệ theo loại')
    st.pyplot(fig) 

def Statics_area(df):
    sum_area = df['area'].sum()
    mean_area = df['area'].mean()
    std_area = df['area'].std()
    with st.container(): 
        st.write("**Tổng diện tích thu thập**")
        st.write(f"<div style='border: 1px solid #ddd; padding: 10px; border-radius: 5px; font-size: 20px; color: #4CAF50;'>{sum_area:,.2f} m² </div>", unsafe_allow_html=True)   
        st.write("**Diện tích trung bình:**")
        st.write(f"<div style='border: 1px solid #ddd; padding: 10px; border-radius: 5px; font-size: 20px; color: #4CAF50;'>{mean_area:,.2f} m²</div>", unsafe_allow_html=True)
        st.write("**Độ lệch diện tích:**")
        st.write(f"<div style='border: 1px solid #ddd; padding: 10px; border-radius: 5px; font-size: 20px; color: #F44336;'>{std_area:,.2f} m²</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    st.set_page_config(
    page_title="Visulization",
    layout="wide"  
    )
    cluster = Cluster(["localhost"])
    session = cluster.connect('real_estate_data')
    query = "SELECT ward as district , price , area ,bed,bath, category,address FROM properties limit 10000"
    rows = session.execute(query)
    rows = [row for row in rows if row.category in ['Chung Cư Thuê','Thuê Nhà Mặt Tiền','Thuê Nhà Trọ','Thuê Nhà Riêng']]
    df = pd.DataFrame(list(rows))
    # Tạo thống kê và bản đồ
    st.title("Bảng Tổng Hợp Dữ Liệu Thuê BĐS (Nguồn BDS68.com.vn)")
    col1, col2 = st.columns(2)
    with col1:
        col3, col4 = st.columns(2)
        with col3 : 
            Statics_price(df)
        with col4:
            Statics_area(df)
        bar_plot_price_m2(df)
        col7,col8 =  st.columns(2)
        with col7:
            pie_chart(df)
        with col8:
            bar_plot_square_area(df)        
    with col2:
        fig = Make_choropleth_map(rows)
        st.write("**Bản đồ giá thuê trung bình của TP.Hồ Chí Minh:**")
        st.plotly_chart(fig)
        st.dataframe(df)
    
