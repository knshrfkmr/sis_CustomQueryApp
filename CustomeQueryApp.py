import streamlit as st
import pandas as pd
# import plotly.express as px
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Complete as Comp
# import re

# ページ設定
st.set_page_config(page_title="Snowflake Data Explorer", layout="wide")

# カスタムCSS
st.markdown("""
<style>
    .stApp {
        max-width: 2100px;
        margin: 0 auto;
    }
    .stSelectbox, .stTextArea {
        margin-bottom: 20px;
    }
    .stButton > button {
        width: 100%;
    }
    .dataframe {
        font-size: 12px;
    }
    h1, h2, h3 {
        margin-top: 30px;
        margin-bottom: 15px;
    }
</style>
""", unsafe_allow_html=True)

# Snowflakeセッションの初期化
@st.cache_resource
def init_session():
    return get_active_session()

session = init_session()
session

 # CortexAI Complete関数を使用してSQLを説明
@st.cache_data
def explain_sql(sql):
    prompt = f"あなたはSQL内容の分析アシスタントです。次のSQLの内容を日本語で解説してください。解説の後には、以下の二つを含めてください。1.SQL内に登場するカラムの意味を推測して説明する。なお、カラム名の推測が難しい場合はその旨を伝える。2.1のカラム名の推測を踏まえて、このSQLの概要をまとめる:\n\n{sql_query}"
    result = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b','{prompt}')").collect()
    return result[0][0]

# @st.cache_data
# def error_explain(error_text):
#     prompt = f"あなたはSQLエラーの分析アシスタントです。次のSQLエラーを分析し、日本語で解決案を提示してください。:\n\n{error_text}"
#     err_result = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b','{prompt}')").collect()
#     return err_result[0][0]


# タブの作成
tab1, tab2= st.tabs(["カスタムSQL", "Under Construction"])

with tab1:
        # サイドバーにデータベース、スキーマ、テーブルの選択を配置
    st.sidebar.header("データ選択")
    
    databases = [row['name'] for row in session.sql("SHOW DATABASES").collect()]
    selected_db = st.sidebar.selectbox("データベースを選択:", databases)
    
    schemas = [row['name'] for row in session.sql(f"SHOW SCHEMAS IN DATABASE {selected_db}").collect()]
    selected_schema = st.sidebar.selectbox("スキーマを選択:", schemas)
    
    tables = [row['name'] for row in session.sql(f"SHOW TABLES IN SCHEMA {selected_db}.{selected_schema}").collect()]
    selected_table = st.sidebar.selectbox("テーブルを選択:", tables)
    
    # メイン画面
    st.title("🔍 Snowflake Data Explorer")
    
    if selected_table:
        # テーブル1のデータを取得し、データフレーム1として表示
        @st.cache_data
        def load_data(table):
            return session.table(f"{selected_db}.{selected_schema}.{table}").limit(1000).to_pandas()
    
        df1 = load_data(selected_table)
        
        st.header(f"📊 {selected_table} データプレビュー")
        st.dataframe(df1, height=300)
         # 行数と列数の表示
        st.info(f"総行数: {len(df1)}, 総列数: {len(df1.columns)}")
    
        st.header("🔍 カスタムクエリ")
        sql_query = st.text_area("SQLクエリを入力してください:", height=300)
    
        if st.button("クエリを実行", key="execute_query"):
            if sql_query:
                st.subheader("実行するSQL:")
                st.code(sql_query, language="sql")
    
                sql_explanation = explain_sql(sql_query)
                st.subheader("SQLの分析:")
                st.info(sql_explanation)
    
                try:
                    with st.spinner('クエリ実行中...'):
                        result = session.sql(sql_query).collect()
                    
                    st.success("クエリが正常に実行されました！")
                    st.subheader("クエリ結果")
                    df2 = pd.DataFrame(result)
                    
                    # データフレーム2の表示（行数制限付き）
                    max_rows = 1000
                    if len(df2) > max_rows:
                        st.warning(f"結果が{max_rows}行を超えています。最初の{max_rows}行のみ表示します。")
                        df2_display = df2.head(max_rows)
                    else:
                        df2_display = df2
                    
                    st.dataframe(df2_display, height=300)
                    
                    # 行数と列数の表示
                    st.info(f"総行数: {len(df2)}, 総列数: {len(df2.columns)}")
                    
                except Exception as e:
                    st.error(f"エラーが発生しました: {str(e)}")
                    error_text = str(e)
                    # error_explanation = error_explain(error_text)
                    # st.subheader("SQLエラーの分析:")
                    # st.info(error_explanation)
            else:
                st.warning("SQLクエリを入力してください。")
    else:
        st.info("👈 サイドバーからデータベース、スキーマ、テーブルを選択してください。")
