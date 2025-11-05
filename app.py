import streamlit as st
import pandas as pd
import os
import time

# Config
st.set_page_config(page_title="Gaming Finance Reconciliation Dashboard", layout="wide")
CHUNK_SIZE = 1_000_000

# Logging
def log_msg(msg):
    os.makedirs("logs", exist_ok=True)
    with open("logs/internal_errors.txt", "a") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {msg}\n")

# Flexible file validator
def validate_file(path, required_cols=None):
    try:
        df = pd.read_csv(path, nrows=100)
        log_msg(f"Columns in {os.path.basename(path)}: {', '.join(df.columns)}")
        if required_cols:
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                log_msg(f"Warning: Missing columns in {os.path.basename(path)}: {', '.join(missing)}")
        return pd.read_csv(path)
    except Exception as e:
        log_msg(f"Validation failed: {str(e)}")
        return None

# Chunked reconciliation
def reconcile_metric(pre_df, post_df, metric):
    if metric not in pre_df.columns or metric not in post_df.columns:
        log_msg(f"Metric {metric} not found in one of the files")
        return pd.DataFrame()

    pre_df = pre_df[["PlayerID", metric]].dropna()
    post_df = post_df[["PlayerID", metric]].dropna()
    pre_df.set_index("PlayerID", inplace=True)
    post_df.set_index("PlayerID", inplace=True)

    common_ids = pre_df.index.intersection(post_df.index)
    result = []

    for i in range(0, len(common_ids), CHUNK_SIZE):
        chunk_ids = common_ids[i:i + CHUNK_SIZE]
        pre_chunk = pre_df.loc[chunk_ids]
        post_chunk = post_df.loc[chunk_ids]

        merged = pre_chunk.join(post_chunk, lsuffix="_pre", rsuffix="_post")
        merged["diff"] = merged[f"{metric}_pre"] - merged[f"{metric}_post"]
        mismatches = merged[merged["diff"] != 0].reset_index()
        mismatches["Metric"] = metric
        result.append(mismatches)

    return pd.concat(result, ignore_index=True) if result else pd.DataFrame()

# LastLoginDate reconciliation
def reconcile_login(pre_ids, post_df):
    post_df = post_df[["PlayerID", "LastLoginDate"]].dropna()
    post_df.set_index("PlayerID", inplace=True)
    missing = [pid for pid in pre_ids if pid not in post_df.index]
    return pd.DataFrame({"PlayerID": missing, "Metric": "LastLoginDate", "Status": "Missing Post"})

# UI
st.title("Gaming Finance Reconciliation Dashboard")

preparer = st.text_input("Report Preparer Name")
reviewer = st.text_input("Report Reviewer Name")

st.markdown("### Upload Files")
col1, col2 = st.columns(2)

with col1:
    st.subheader("Pre-Migration Files")
    pre_file = st.file_uploader("Pre File (PlayerID, InteractiveBalance, SubscriptionBalance)", type="csv", key="pre")
    pre_login_file = st.file_uploader("Pre LastLogin File (optional)", type="csv", key="prelogin")

with col2:
    st.subheader("Post-Migration Files")
    post_int_file = st.file_uploader("Post InteractiveBalance File", type="csv", key="int")
    post_sub_file = st.file_uploader("Post SubscriptionBalance File", type="csv", key="sub")
    post_login_file = st.file_uploader("Post LastLoginDate File", type="csv", key="login")

summary_placeholder = st.empty()

if st.button("Run Reconciliation"):
    if not pre_file:
        st.error("Please upload the Pre-Migration file.")
    elif not preparer or not reviewer:
        st.error("Please enter both preparer and reviewer names.")
    else:
        with st.spinner("Processing..."):
            ts = time.strftime("%Y%m%d_%H%M%S")
            paths = {}
            for label, file in zip(
                ["pre", "prelogin", "int", "sub", "login"],
                [pre_file, pre_login_file, post_int_file, post_sub_file, post_login_file]
            ):
                if file:
                    path = f"temp_{label}_{ts}.csv"
                    with open(path, "wb") as f: f.write(file.getbuffer())
                    paths[label] = path

            pre_df = validate_file(paths.get("pre"))
            pre_login_df = validate_file(paths.get("prelogin")) if "prelogin" in paths else None
            int_df = validate_file(paths.get("int")) if "int" in paths else None
            sub_df = validate_file(paths.get("sub")) if "sub" in paths else None
            login_df = validate_file(paths.get("login")) if "login" in paths else None

            if pre_df is None:
                st.error("Pre file validation failed. See logs/internal_errors.txt.")
            else:
                results = []

                if int_df is not None:
                    results.append(reconcile_metric(pre_df, int_df, "InteractiveBalance"))
                if sub_df is not None:
                    results.append(reconcile_metric(pre_df, sub_df, "SubscriptionBalance"))
                if login_df is not None and pre_login_df is not None:
                    results.append(reconcile_metric(pre_login_df, login_df, "LastLoginDate"))
                elif login_df is not None:
                    results.append(reconcile_login(pre_df["PlayerID"].tolist(), login_df))

                all_ex = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

                if not all_ex.empty:
                    summary_text = f"""
                    ### 🧾 Reconciliation Summary  
                    - **Prepared by**: {preparer}  
                    - **Reviewed by**: {reviewer}  
                    - **Timestamp**: {ts}  
                    - **Total Exceptions**: {len(all_ex)}  
                    - **Metrics with Exceptions**: {', '.join(sorted(all_ex['Metric'].unique()))}
                    """
                    summary_placeholder.markdown(summary_text)
                    st.dataframe(all_ex, use_container_width=True)

                    filename = f"exceptions_{preparer.replace(' ', '_')}_{ts}.csv"
                    st.download_button(
                        label="Download Exceptions",
                        data=all_ex.to_csv(index=False).encode("utf-8"),
                        file_name=filename,
                        mime="text/csv"
                    )
                else:
                    summary_text = f"""
                    ### ✅ No mismatches found  
                    - **Prepared by**: {preparer}  
                    - **Reviewed by**: {reviewer}  
                    - **Timestamp**: {ts}  
                    - You may take a screenshot of this message as audit evidence.
                    """
                    summary_placeholder.markdown(summary_text)

            for p in paths.values():
                os.remove(p)
