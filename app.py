
import sqlite3
from pathlib import Path
from datetime import date, timedelta
import shutil
import pandas as pd
import streamlit as st

APP_TITLE = "営業CRM 体験型デモ"
BASE_DIR = Path(__file__).parent
SEED_DB_PATH = BASE_DIR / "sample_crm_demo.db"

RUNTIME_DB_PATH = Path("/tmp/crm_experience_demo_runtime.db")
if not Path("/tmp").exists():
    RUNTIME_DB_PATH = BASE_DIR / "crm_experience_demo_runtime.db"

CUSTOMER_STATUS = ["未接触", "接触済", "商談中", "契約済", "休眠", "失注"]
INDUSTRIES = ["運送", "建設", "外食", "宿泊", "介護", "製造", "農業", "その他"]
DEAL_PHASES = ["初回接触", "ヒアリング", "提案", "見積", "契約調整", "契約済", "失注"]
TEMPERATURES = ["HOT", "WARM", "COLD"]
ACTIVITY_TYPES = ["電話", "メール", "Web会議", "訪問", "紹介", "資料送付", "その他"]
NATIONALITIES = ["ベトナム", "インドネシア", "ミャンマー", "中国", "ネパール", "フィリピン", "その他"]
VISA_TYPES = ["特定技能1号", "技術・人文知識・国際業務", "技能実習", "留学", "家族滞在", "その他"]
WORKER_STATUS = ["未紹介", "紹介中", "面談中", "内定", "入社済", "辞退", "保留"]
REVENUE_TYPES = ["紹介料", "支援費", "講習費", "その他"]
REVENUE_STATUS = ["見込", "請求済", "入金済", "失注"]
COST_TYPES = ["VISA申請費", "外免切替費", "講習費", "住居関連費", "渡航費", "翻訳費", "その他"]

st.set_page_config(page_title=APP_TITLE, layout="wide")


def create_schema(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT NOT NULL,
        industry TEXT,
        sales_owner TEXT,
        status TEXT,
        last_contact_date TEXT,
        next_action_date TEXT,
        transport_permit TEXT,
        gmark TEXT,
        memo TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS deals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        deal_name TEXT NOT NULL,
        phase TEXT,
        temperature TEXT,
        expected_amount INTEGER DEFAULT 0,
        probability INTEGER DEFAULT 0,
        next_action_date TEXT,
        sales_owner TEXT,
        memo TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS activities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        deal_id INTEGER,
        activity_date TEXT,
        activity_type TEXT,
        detail TEXT,
        next_action TEXT,
        next_action_date TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS workers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        worker_name TEXT NOT NULL,
        nationality TEXT,
        visa_type TEXT,
        japanese_level TEXT,
        work_field TEXT,
        status TEXT,
        visa_expire_date TEXT,
        customer_id INTEGER,
        memo TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS revenues (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        deal_id INTEGER,
        worker_id INTEGER,
        revenue_type TEXT,
        planned_date TEXT,
        actual_date TEXT,
        expected_amount INTEGER DEFAULT 0,
        actual_amount INTEGER DEFAULT 0,
        status TEXT,
        memo TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS costs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id INTEGER,
        deal_id INTEGER,
        worker_id INTEGER,
        cost_type TEXT,
        cost_date TEXT,
        expected_cost INTEGER DEFAULT 0,
        actual_cost INTEGER DEFAULT 0,
        payee TEXT,
        memo TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()


def create_seed_db(path):
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    create_schema(conn)
    cur = conn.cursor()
    today = date.today()

    cur.executemany("""
    INSERT INTO customers (
        company_name, industry, sales_owner, status, last_contact_date, next_action_date,
        transport_permit, gmark, memo
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        ("株式会社サンプル物流", "運送", "山田", "商談中", str(today - timedelta(days=1)), str(today), "有", "確認中", "特定技能ドライバー採用を検討中"),
        ("東海ロジスティクス株式会社", "運送", "佐藤", "接触済", str(today - timedelta(days=3)), str(today + timedelta(days=2)), "有", "有", "登録支援委託に関心あり"),
        ("関東フードサービス株式会社", "外食", "山田", "未接触", None, str(today + timedelta(days=5)), "確認中", "無", "外国人採用経験あり"),
        ("中部建設株式会社", "建設", "田中", "商談中", str(today - timedelta(days=2)), str(today + timedelta(days=1)), "確認中", "確認中", "建設分野の特定技能に関心あり"),
    ])

    cur.executemany("""
    INSERT INTO deals (
        customer_id, deal_name, phase, temperature, expected_amount, probability,
        next_action_date, sales_owner, memo
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (1, "特定技能ドライバー紹介", "提案", "HOT", 1200000, 70, str(today), "山田", "2名紹介予定。受入体制確認中。"),
        (2, "登録支援委託相談", "ヒアリング", "WARM", 600000, 50, str(today + timedelta(days=2)), "佐藤", "費用体系の説明待ち。"),
        (3, "外食人材紹介", "初回接触", "COLD", 450000, 30, str(today + timedelta(days=5)), "山田", "初回架電予定。"),
        (4, "建設分野 特定技能紹介", "見積", "HOT", 900000, 60, str(today + timedelta(days=1)), "田中", "現場要件の確認が必要。"),
    ])

    cur.executemany("""
    INSERT INTO activities (
        customer_id, deal_id, activity_date, activity_type, detail, next_action, next_action_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        (1, 1, str(today), "電話", "特定技能ドライバーの不足状況をヒアリング。", "概算見積と導入手順を送付", str(today + timedelta(days=1))),
        (2, 2, str(today - timedelta(days=1)), "メール", "セミナー参加後のお礼メールを送付。", "電話フォロー", str(today + timedelta(days=2))),
        (4, 4, str(today - timedelta(days=2)), "Web会議", "建設現場で必要な日本語レベルと経験を確認。", "候補者要件表を送付", str(today + timedelta(days=1))),
    ])

    cur.executemany("""
    INSERT INTO workers (
        worker_name, nationality, visa_type, japanese_level, work_field, status,
        visa_expire_date, customer_id, memo
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        ("Nguyen Van A", "ベトナム", "特定技能1号", "N4", "運送", "紹介中", str(today + timedelta(days=240)), 1, "ドライバー職希望"),
        ("Siti B", "インドネシア", "特定技能1号", "N3", "外食", "未紹介", str(today + timedelta(days=180)), None, "外食経験あり"),
        ("Min Thu C", "ミャンマー", "特定技能1号", "N4", "建設", "面談中", str(today + timedelta(days=210)), 4, "建設経験あり"),
    ])

    cur.executemany("""
    INSERT INTO revenues (
        customer_id, deal_id, worker_id, revenue_type, planned_date, actual_date,
        expected_amount, actual_amount, status, memo
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (1, 1, 1, "紹介料", str(today + timedelta(days=30)), None, 1200000, 0, "見込", "入社決定後に請求予定"),
        (2, 2, None, "支援費", str(today + timedelta(days=45)), None, 600000, 0, "見込", "月額支援費の年間見込み"),
        (4, 4, 3, "紹介料", str(today + timedelta(days=35)), None, 900000, 0, "見込", "内定後に請求予定"),
    ])

    cur.executemany("""
    INSERT INTO costs (
        customer_id, deal_id, worker_id, cost_type, cost_date, expected_cost,
        actual_cost, payee, memo
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (1, 1, 1, "VISA申請費", str(today + timedelta(days=10)), 80000, 0, "行政書士", "申請予定"),
        (1, 1, 1, "外免切替費", str(today + timedelta(days=20)), 150000, 0, "教習所", "概算"),
        (4, 4, 3, "講習費", str(today + timedelta(days=15)), 120000, 0, "講習機関", "見込み"),
    ])
    conn.commit()
    conn.close()


def copy_seed_to_runtime(force=False):
    if force or not RUNTIME_DB_PATH.exists():
        if SEED_DB_PATH.exists():
            shutil.copyfile(SEED_DB_PATH, RUNTIME_DB_PATH)
        else:
            create_seed_db(RUNTIME_DB_PATH)


def connect():
    copy_seed_to_runtime()
    return sqlite3.connect(RUNTIME_DB_PATH, check_same_thread=False)


def execute(sql, params=None):
    conn = connect()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    conn.commit()
    conn.close()


def read_df(sql, params=None):
    conn = connect()
    df = pd.read_sql(sql, conn, params=params or [])
    conn.close()
    return df


def fmt_yen(value):
    try:
        return f"¥{int(value):,}"
    except Exception:
        return "¥0"


def csv_download(df, filename, label):
    csv = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(label=label, data=csv, file_name=filename, mime="text/csv")


def get_customer_options(include_blank=False):
    df = read_df("SELECT id, company_name FROM customers ORDER BY company_name")
    opts = {f"{row.company_name}（ID:{row.id}）": int(row.id) for row in df.itertuples()}
    if include_blank:
        return {"未選択": None} | opts
    return opts


def get_deal_options(customer_id=None, include_blank=False):
    if customer_id:
        df = read_df("SELECT id, deal_name FROM deals WHERE customer_id = ? ORDER BY id DESC", [customer_id])
    else:
        df = read_df("SELECT id, deal_name FROM deals ORDER BY id DESC")
    opts = {f"{row.deal_name}（ID:{row.id}）": int(row.id) for row in df.itertuples()}
    if include_blank:
        return {"未選択": None} | opts
    return opts


def get_worker_options():
    df = read_df("SELECT id, worker_name FROM workers ORDER BY worker_name")
    return {"未選択": None} | {f"{row.worker_name}（ID:{row.id}）": int(row.id) for row in df.itertuples()}


def common_header():
    st.title(APP_TITLE)
    st.warning(
        "これは体験型デモ環境です。本番データ、実在する個人情報、顧客情報、契約情報、売上情報は入力しないでください。"
        "入力内容は一時保存であり、再起動・再デプロイ・リセット操作で消える可能性があります。"
    )


def page_home():
    common_header()
    today = str(date.today())

    customers = read_df("SELECT * FROM customers")
    deals = read_df("SELECT * FROM deals")
    revenues = read_df("SELECT * FROM revenues")
    costs = read_df("SELECT * FROM costs")

    due_deals = deals[(deals["next_action_date"].fillna("") <= today) & (~deals["phase"].isin(["契約済", "失注"]))]
    hot_deals = deals[(deals["temperature"] == "HOT") & (~deals["phase"].isin(["契約済", "失注"]))]
    forecast = (deals[deals["phase"] != "失注"]["expected_amount"] * deals[deals["phase"] != "失注"]["probability"] / 100).sum()
    gross_profit = revenues["expected_amount"].sum() - costs["expected_cost"].sum()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("顧客数", len(customers))
    c2.metric("本日の対応", len(due_deals))
    c3.metric("HOT商談", len(hot_deals))
    c4.metric("売上見込み", fmt_yen(forecast))
    c5.metric("粗利見込み", fmt_yen(gross_profit))

    st.subheader("本日の優先対応")
    due = read_df("""
        SELECT d.id AS 商談ID, c.company_name AS 顧客名, d.deal_name AS 商談名,
               d.phase AS フェーズ, d.temperature AS 温度感,
               d.expected_amount AS 見込金額, d.probability AS 確度,
               d.next_action_date AS 次回対応日
        FROM deals d
        LEFT JOIN customers c ON d.customer_id = c.id
        WHERE d.next_action_date <= ? AND d.phase NOT IN ('契約済', '失注')
        ORDER BY d.next_action_date ASC
    """, [today])
    st.dataframe(due, use_container_width=True, hide_index=True)

    st.subheader("直近の活動履歴")
    activities = read_df("""
        SELECT a.activity_date AS 活動日, c.company_name AS 顧客名, d.deal_name AS 商談名,
               a.activity_type AS 活動種別, a.detail AS 活動内容,
               a.next_action AS 次回対応, a.next_action_date AS 次回対応日
        FROM activities a
        LEFT JOIN customers c ON a.customer_id = c.id
        LEFT JOIN deals d ON a.deal_id = d.id
        ORDER BY a.activity_date DESC, a.id DESC
        LIMIT 10
    """)
    st.dataframe(activities, use_container_width=True, hide_index=True)


def page_customers():
    common_header()
    st.header("顧客管理")

    with st.expander("新規顧客を登録する", expanded=True):
        with st.form("customer_form"):
            col1, col2 = st.columns(2)
            company_name = col1.text_input("会社名", placeholder="デモ会社名を入力")
            industry = col2.selectbox("業種", INDUSTRIES)
            sales_owner = col1.text_input("営業担当", value="デモ担当")
            status = col2.selectbox("顧客ステータス", CUSTOMER_STATUS, index=2)
            last_contact_date = col1.date_input("最終接触日", value=date.today())
            next_action_date = col2.date_input("次回対応日", value=date.today() + timedelta(days=7))
            transport_permit = col1.selectbox("一般貨物許可", ["確認中", "有", "無"])
            gmark = col2.selectbox("Gマーク", ["確認中", "有", "無"])
            memo = st.text_area("備考")
            submitted = st.form_submit_button("顧客を登録")

        if submitted:
            if not company_name.strip():
                st.error("会社名を入力してください。")
            else:
                execute("""
                    INSERT INTO customers (
                        company_name, industry, sales_owner, status, last_contact_date,
                        next_action_date, transport_permit, gmark, memo
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [company_name, industry, sales_owner, status, str(last_contact_date), str(next_action_date), transport_permit, gmark, memo])
                st.success("顧客を登録しました。")
                st.rerun()

    st.subheader("顧客一覧")
    col1, col2 = st.columns([2, 1])
    keyword = col1.text_input("会社名検索")
    status_filter = col2.selectbox("ステータス絞り込み", ["すべて"] + CUSTOMER_STATUS)

    df = read_df("""
        SELECT id AS ID, company_name AS 会社名, industry AS 業種, sales_owner AS 営業担当,
               status AS 顧客ステータス, last_contact_date AS 最終接触日,
               next_action_date AS 次回対応日, transport_permit AS 一般貨物許可,
               gmark AS Gマーク, memo AS 備考
        FROM customers
        ORDER BY id DESC
    """)
    if keyword:
        df = df[df["会社名"].str.contains(keyword, na=False)]
    if status_filter != "すべて":
        df = df[df["顧客ステータス"] == status_filter]
    st.dataframe(df, use_container_width=True, hide_index=True)
    csv_download(df, "customers_demo.csv", "顧客CSVをダウンロード")


def page_deals():
    common_header()
    st.header("商談管理")
    customer_options = get_customer_options()

    with st.expander("新規商談を登録する", expanded=True):
        if not customer_options:
            st.info("先に顧客を登録してください。")
        else:
            with st.form("deal_form"):
                selected_customer = st.selectbox("顧客", list(customer_options.keys()))
                customer_id = customer_options[selected_customer]
                deal_name = st.text_input("商談名", value="デモ商談")
                col1, col2 = st.columns(2)
                phase = col1.selectbox("商談フェーズ", DEAL_PHASES, index=2)
                temperature = col2.selectbox("温度感", TEMPERATURES)
                expected_amount = col1.number_input("見込金額", min_value=0, step=10000, value=600000)
                probability = col2.number_input("確度（%）", min_value=0, max_value=100, step=5, value=50)
                next_action_date = col1.date_input("次回対応日", value=date.today() + timedelta(days=7))
                sales_owner = col2.text_input("営業担当", value="デモ担当")
                memo = st.text_area("商談メモ")
                submitted = st.form_submit_button("商談を登録")

            if submitted:
                execute("""
                    INSERT INTO deals (
                        customer_id, deal_name, phase, temperature, expected_amount,
                        probability, next_action_date, sales_owner, memo
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [customer_id, deal_name, phase, temperature, int(expected_amount), int(probability), str(next_action_date), sales_owner, memo])
                st.success("商談を登録しました。")
                st.rerun()

    st.subheader("商談一覧")
    df = read_df("""
        SELECT d.id AS 商談ID, c.company_name AS 顧客名, d.deal_name AS 商談名,
               d.phase AS フェーズ, d.temperature AS 温度感,
               d.expected_amount AS 見込金額, d.probability AS 確度,
               ROUND(d.expected_amount * d.probability / 100.0, 0) AS 確度加味金額,
               d.next_action_date AS 次回対応日, d.sales_owner AS 営業担当, d.memo AS メモ
        FROM deals d
        LEFT JOIN customers c ON d.customer_id = c.id
        ORDER BY d.id DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)
    csv_download(df, "deals_demo.csv", "商談CSVをダウンロード")


def page_activities():
    common_header()
    st.header("活動履歴")
    customer_options = get_customer_options()

    with st.expander("営業活動を登録する", expanded=True):
        if not customer_options:
            st.info("先に顧客を登録してください。")
        else:
            selected_customer = st.selectbox("顧客", list(customer_options.keys()), key="act_customer")
            customer_id = customer_options[selected_customer]
            deal_options = get_deal_options(customer_id, include_blank=True)

            with st.form("activity_form"):
                selected_deal = st.selectbox("商談", list(deal_options.keys()))
                deal_id = deal_options[selected_deal]
                col1, col2 = st.columns(2)
                activity_date = col1.date_input("活動日", value=date.today())
                activity_type = col2.selectbox("活動種別", ACTIVITY_TYPES)
                detail = st.text_area("活動内容", placeholder="デモ用の内容を入力してください")
                next_action = st.text_area("次回対応")
                next_action_date = st.date_input("次回対応日", value=date.today() + timedelta(days=7))
                submitted = st.form_submit_button("活動履歴を登録")

            if submitted:
                if not detail.strip():
                    st.error("活動内容を入力してください。")
                else:
                    execute("""
                        INSERT INTO activities (
                            customer_id, deal_id, activity_date, activity_type,
                            detail, next_action, next_action_date
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, [customer_id, deal_id, str(activity_date), activity_type, detail, next_action, str(next_action_date)])
                    execute("UPDATE customers SET last_contact_date = ?, next_action_date = ? WHERE id = ?",
                            [str(activity_date), str(next_action_date), customer_id])
                    if deal_id:
                        execute("UPDATE deals SET next_action_date = ? WHERE id = ?",
                                [str(next_action_date), deal_id])
                    st.success("活動履歴を登録しました。顧客・商談の次回対応日も更新しました。")
                    st.rerun()

    st.subheader("活動履歴一覧")
    df = read_df("""
        SELECT a.id AS 活動ID, a.activity_date AS 活動日, c.company_name AS 顧客名,
               d.deal_name AS 商談名, a.activity_type AS 活動種別,
               a.detail AS 活動内容, a.next_action AS 次回対応,
               a.next_action_date AS 次回対応日
        FROM activities a
        LEFT JOIN customers c ON a.customer_id = c.id
        LEFT JOIN deals d ON a.deal_id = d.id
        ORDER BY a.activity_date DESC, a.id DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)
    csv_download(df, "activities_demo.csv", "活動履歴CSVをダウンロード")


def page_workers():
    common_header()
    st.header("紹介人材管理")
    customer_options = get_customer_options(include_blank=True)

    with st.expander("紹介人材を登録する", expanded=True):
        with st.form("worker_form"):
            col1, col2 = st.columns(2)
            worker_name = col1.text_input("氏名", placeholder="デモ氏名を入力")
            nationality = col2.selectbox("国籍", NATIONALITIES)
            visa_type = col1.selectbox("在留資格", VISA_TYPES)
            japanese_level = col2.selectbox("日本語レベル", ["N1", "N2", "N3", "N4", "N5", "不明"])
            work_field = col1.selectbox("分野", INDUSTRIES)
            status = col2.selectbox("人材ステータス", WORKER_STATUS)
            visa_expire_date = col1.date_input("在留期限", value=date.today() + timedelta(days=180))
            selected_customer = col2.selectbox("紹介先顧客", list(customer_options.keys()))
            customer_id = customer_options[selected_customer]
            memo = st.text_area("備考")
            submitted = st.form_submit_button("人材を登録")

        if submitted:
            if not worker_name.strip():
                st.error("氏名を入力してください。")
            else:
                execute("""
                    INSERT INTO workers (
                        worker_name, nationality, visa_type, japanese_level, work_field,
                        status, visa_expire_date, customer_id, memo
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [worker_name, nationality, visa_type, japanese_level, work_field, status, str(visa_expire_date), customer_id, memo])
                st.success("紹介人材を登録しました。")
                st.rerun()

    st.subheader("紹介人材一覧")
    df = read_df("""
        SELECT w.id AS 人材ID, w.worker_name AS 氏名, w.nationality AS 国籍,
               w.visa_type AS 在留資格, w.japanese_level AS 日本語レベル,
               w.work_field AS 分野, w.status AS ステータス,
               w.visa_expire_date AS 在留期限, c.company_name AS 紹介先顧客,
               w.memo AS 備考
        FROM workers w
        LEFT JOIN customers c ON w.customer_id = c.id
        ORDER BY w.id DESC
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)
    csv_download(df, "workers_demo.csv", "紹介人材CSVをダウンロード")


def page_revenue_cost():
    common_header()
    st.header("売上・原価管理")
    customer_options = get_customer_options()
    deal_options = get_deal_options(include_blank=True)
    worker_options = get_worker_options()

    tab1, tab2, tab3 = st.tabs(["売上登録", "原価登録", "粗利確認"])

    with tab1:
        if customer_options:
            with st.form("revenue_form"):
                selected_customer = st.selectbox("顧客", list(customer_options.keys()), key="rev_customer")
                customer_id = customer_options[selected_customer]
                selected_deal = st.selectbox("商談", list(deal_options.keys()), key="rev_deal")
                deal_id = deal_options[selected_deal]
                selected_worker = st.selectbox("人材", list(worker_options.keys()), key="rev_worker")
                worker_id = worker_options[selected_worker]
                col1, col2 = st.columns(2)
                revenue_type = col1.selectbox("売上区分", REVENUE_TYPES)
                status = col2.selectbox("売上ステータス", REVENUE_STATUS)
                planned_date = col1.date_input("売上予定日", value=date.today() + timedelta(days=30))
                actual_amount = col2.number_input("実績金額", min_value=0, step=10000, value=0)
                expected_amount = col1.number_input("見込金額", min_value=0, step=10000, value=600000)
                memo = st.text_area("備考")
                submitted = st.form_submit_button("売上を登録")
            if submitted:
                execute("""
                    INSERT INTO revenues (
                        customer_id, deal_id, worker_id, revenue_type, planned_date,
                        actual_date, expected_amount, actual_amount, status, memo
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [customer_id, deal_id, worker_id, revenue_type, str(planned_date), None, int(expected_amount), int(actual_amount), status, memo])
                st.success("売上を登録しました。")
                st.rerun()

        df = read_df("""
            SELECT r.id AS 売上ID, c.company_name AS 顧客名, d.deal_name AS 商談名,
                   w.worker_name AS 人材名, r.revenue_type AS 売上区分,
                   r.planned_date AS 売上予定日, r.expected_amount AS 見込金額,
                   r.actual_amount AS 実績金額, r.status AS ステータス, r.memo AS 備考
            FROM revenues r
            LEFT JOIN customers c ON r.customer_id = c.id
            LEFT JOIN deals d ON r.deal_id = d.id
            LEFT JOIN workers w ON r.worker_id = w.id
            ORDER BY r.id DESC
        """)
        st.dataframe(df, use_container_width=True, hide_index=True)
        csv_download(df, "revenues_demo.csv", "売上CSVをダウンロード")

    with tab2:
        if customer_options:
            with st.form("cost_form"):
                selected_customer = st.selectbox("顧客", list(customer_options.keys()), key="cost_customer")
                customer_id = customer_options[selected_customer]
                selected_deal = st.selectbox("商談", list(deal_options.keys()), key="cost_deal")
                deal_id = deal_options[selected_deal]
                selected_worker = st.selectbox("人材", list(worker_options.keys()), key="cost_worker")
                worker_id = worker_options[selected_worker]
                col1, col2 = st.columns(2)
                cost_type = col1.selectbox("原価項目", COST_TYPES)
                cost_date = col2.date_input("発生日", value=date.today())
                expected_cost = col1.number_input("見込原価", min_value=0, step=10000, value=80000)
                actual_cost = col2.number_input("実績原価", min_value=0, step=10000, value=0)
                payee = col1.text_input("支払先", value="")
                memo = st.text_area("備考", key="cost_memo")
                submitted = st.form_submit_button("原価を登録")
            if submitted:
                execute("""
                    INSERT INTO costs (
                        customer_id, deal_id, worker_id, cost_type, cost_date,
                        expected_cost, actual_cost, payee, memo
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [customer_id, deal_id, worker_id, cost_type, str(cost_date), int(expected_cost), int(actual_cost), payee, memo])
                st.success("原価を登録しました。")
                st.rerun()

        df = read_df("""
            SELECT co.id AS 原価ID, c.company_name AS 顧客名, d.deal_name AS 商談名,
                   w.worker_name AS 人材名, co.cost_type AS 原価項目,
                   co.cost_date AS 発生日, co.expected_cost AS 見込原価,
                   co.actual_cost AS 実績原価, co.payee AS 支払先, co.memo AS 備考
            FROM costs co
            LEFT JOIN customers c ON co.customer_id = c.id
            LEFT JOIN deals d ON co.deal_id = d.id
            LEFT JOIN workers w ON co.worker_id = w.id
            ORDER BY co.id DESC
        """)
        st.dataframe(df, use_container_width=True, hide_index=True)
        csv_download(df, "costs_demo.csv", "原価CSVをダウンロード")

    with tab3:
        df = read_df("""
            SELECT
                c.company_name AS 顧客名,
                COALESCE(SUM(DISTINCT r.expected_amount), 0) AS 売上見込,
                COALESCE(SUM(co.expected_cost), 0) AS 原価見込,
                COALESCE(SUM(DISTINCT r.expected_amount), 0) - COALESCE(SUM(co.expected_cost), 0) AS 粗利見込
            FROM customers c
            LEFT JOIN revenues r ON c.id = r.customer_id
            LEFT JOIN costs co ON c.id = co.customer_id
            GROUP BY c.id, c.company_name
            ORDER BY 粗利見込 DESC
        """)
        st.dataframe(df, use_container_width=True, hide_index=True)
        csv_download(df, "gross_profit_demo.csv", "粗利CSVをダウンロード")


def page_csv():
    common_header()
    st.header("CSV出力")
    mapping = {
        "顧客管理": "customers",
        "商談管理": "deals",
        "活動履歴": "activities",
        "紹介人材管理": "workers",
        "売上管理": "revenues",
        "原価管理": "costs",
    }
    for label, table in mapping.items():
        st.subheader(label)
        df = read_df(f"SELECT * FROM {table}")
        st.dataframe(df, use_container_width=True, hide_index=True)
        csv_download(df, f"{table}_demo.csv", f"{label}CSVをダウンロード")


def page_reset():
    common_header()
    st.header("デモ環境リセット")
    st.info("入力したデモデータを消して、GitHub上のサンプルDBから初期状態に戻します。")
    if st.button("サンプルデータに戻す", type="primary"):
        copy_seed_to_runtime(force=True)
        st.success("サンプルデータに戻しました。")
        st.rerun()


def main():
    if not SEED_DB_PATH.exists():
        create_seed_db(SEED_DB_PATH)
    copy_seed_to_runtime()

    st.sidebar.title("メニュー")
    page = st.sidebar.radio(
        "画面を選択",
        ["ホーム", "顧客管理", "商談管理", "活動履歴", "紹介人材管理", "売上・原価管理", "CSV出力", "リセット"]
    )
    st.sidebar.markdown("---")
    st.sidebar.caption("体験型デモ：入力内容は一時DBに保存されます。永続保存は保証されません。")

    if page == "ホーム":
        page_home()
    elif page == "顧客管理":
        page_customers()
    elif page == "商談管理":
        page_deals()
    elif page == "活動履歴":
        page_activities()
    elif page == "紹介人材管理":
        page_workers()
    elif page == "売上・原価管理":
        page_revenue_cost()
    elif page == "CSV出力":
        page_csv()
    elif page == "リセット":
        page_reset()


if __name__ == "__main__":
    main()
