import streamlit as st
import json
import re
import pandas as pd
import _snowflake

from snowflake.snowpark.context import get_active_session
from bin_request_retrieval import fetch_bin_requests, mark_request_read
from call_here_api import (
    call_routing_here_api,
    call_geocoding_here_api,
    decode_polyline,
    display_map,
)

session = get_active_session()

API_ENDPOINT = "/api/v2/cortex/agent:run"
API_TIMEOUT  = 50_000  # milliseconds

SEMANTIC_MODELS        = "@pnp.etremblay.models/sales_metrics_model.yaml"
CORTEX_SEARCH_SERVICES = "pnp.etremblay.sales_conversation_search"
CORTEX_MODEL           = "claude-4-sonnet"


def process_sse_response(events):
    """Parse SSE events into (text, sql, citations)."""
    text, sql, citations = "", "", []
    for evt in events:
        if evt.get("event") == "message.delta":
            for c in evt["data"]["delta"].get("content", []):
                if c["type"] == "text":
                    text += c["text"]
                elif c["type"] == "tool_results":
                    for r in c["tool_results"]["content"]:
                        if r["type"] == "json":
                            j = r["json"]
                            text += j.get("text", "")
                            sql = j.get("sql", sql)
                            for sr in j.get("searchResults", []):
                                citations.append({
                                    "source_id": sr.get("source_id",""),
                                    "doc_id":    sr.get("doc_id","")
                                })
    return text.strip(), sql.strip(), citations


def run_snowflake_query(sql):
    try:
        return session.sql(sql.replace(";", ""))
    except Exception as e:
        st.error(f"SQL error: {e}")
        return None


def extract_addresses(text: str) -> list[str]:
    prompt = (
        "Extract every full street address from this text and output only "
        "a JSON array of strings (no markdown). Example:\n"
        '["123 Main St City, ST 12345", "456 Rue Example Montréal QC H2X 1Y4"]\n\n'
        f"Text:\n```{text}```"
    )
    payload = {
        "model": CORTEX_MODEL,
        "messages": [
            {"role":"user","content":[{"type":"text","text":prompt}]}
        ],
    }
    resp = _snowflake.send_snow_api_request(
        "POST", API_ENDPOINT, {}, {}, payload, None, API_TIMEOUT
    )
    if resp.get("status") != 200:
        st.error(f"Agent error: {resp.get('status')}")
        return []
    try:
        events = json.loads(resp.get("content","[]"))
    except json.JSONDecodeError:
        return []
    full_text, _, _ = process_sse_response(events)
    cleaned = re.sub(r"```(?:json)?","", full_text, flags=re.IGNORECASE).strip()
    m = re.search(r"\[.*\]", cleaned, flags=re.DOTALL)
    if not m:
        return []
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return []


def geocode_address(addr: str):
    try:
        geo = call_geocoding_here_api(addr)
        items = geo.get("items") or []
        if not items:
            return None, None
        pos = items[0]["position"]
        return pos["lat"], pos["lng"]
    except Exception as e:
        st.error(f"Geocoding failed: {e}")
        return None, None


def handle_address_logic(query: str, assistant_text: str) -> bool:
    """
    1) Ask Cortex to extract addresses from the **user’s query**.
    2) Fallback on "between ... and ...".
    3) If 1 address → geocode + st.map
    4) If 2 addresses → geocode + routing + display_map
    Returns True if we handled it here (and should skip the agent).
    """
    addrs = extract_addresses(query)
    if not addrs:
        m = re.search(r"between\s+(.*?)\s+and\s+(.*)", query, flags=re.IGNORECASE)
        if m:
            addrs = [m.group(1).strip(" ,."), m.group(2).strip(" ,.")]
    st.write("🔍 extracted addresses:", addrs)

    # Single-point map
    if len(addrs) == 1:
        lat, lon = geocode_address(addrs[0])
        if lat is not None:
            st.write(f"📍 Map for: **{addrs[0]}**")
            st.map(pd.DataFrame({"lat":[lat],"lon":[lon]}))
        return True

    # Route between two points
    if len(addrs) == 2:
        lat1, lon1 = geocode_address(addrs[0])
        lat2, lon2 = geocode_address(addrs[1])
        if None in (lat1, lon1, lat2, lon2):
            st.error("Could not geocode one or both addresses.")
            return True
        here_json = call_routing_here_api((lat1, lon1), (lat2, lon2))
        coords    = decode_polyline(here_json)
        display_map(coords)
        return True

    # nothing to do
    return False


def snowflake_api_call(prompt: str, limit: int = 5):
    """Call Cortex with only the two supported tools."""
    payload = {
        "model": CORTEX_MODEL,
        "messages":[{"role":"user","content":[{"type":"text","text":prompt}]}],
        "tool_choice": {"type":"auto"},
        "tools": [
            {"tool_spec":{"type":"cortex_analyst_text_to_sql","name":"analyst1"}},
            {"tool_spec":{"type":"cortex_search","name":"search1"}},
        ],
        "tool_resources": {
            "analyst1": {"semantic_model_file": SEMANTIC_MODELS},
            "search1": {
                "name":        CORTEX_SEARCH_SERVICES,
                "max_results": limit,
                "id_column":   "conversation_id"
            }
        }
    }
    resp = _snowflake.send_snow_api_request(
        "POST", API_ENDPOINT, {}, {}, payload, None, API_TIMEOUT
    )
    if resp.get("status") != 200:
        st.error(f"Agent HTTP error: {resp.get('status')}")
        st.write("🔍 Raw agent response:", resp)
        return []
    try:
        return json.loads(resp["content"])
    except json.JSONDecodeError as e:
        st.error(f"Failed to parse response JSON: {e}")
        return []


def direct_completion(prompt: str) -> str:
    """Fallback pure-text completion (no tools)."""
    payload = {
        "model": CORTEX_MODEL,
        "messages":[{"role":"user","content":[{"type":"text","text":prompt}]}]
    }
    resp = _snowflake.send_snow_api_request(
        "POST", API_ENDPOINT, {}, {}, payload, None, API_TIMEOUT
    )
    if resp.get("status") != 200:
        st.error(f"Completion HTTP error: {resp.get('status')}")
        return ""
    try:
        events = json.loads(resp["content"])
    except json.JSONDecodeError:
        return ""
    text = ""
    for evt in events:
        if evt.get("event") == "message.delta":
            for c in evt["data"]["delta"].get("content", []):
                if c["type"] == "text":
                    text += c["text"]
    return text.strip()


def main():
    st.title("🚚 Bin Management & Mapping Assistant")
    tab1, tab2 = st.tabs(["Review Requests","Assistant & Maps"])

    # ── Tab 1: Bin request approval
    with tab1:
        st.header("📥 Review New Bin Requests")
        if "req_idx" not in st.session_state:
            st.session_state.req_idx = 0

        requests = fetch_bin_requests()
        idx      = st.session_state.req_idx

        if not requests:
            st.success("🎉 No new bin requests.")
        elif idx >= len(requests):
            st.success("🎉 All reviewed.")
        else:
            req = requests[idx]
            mid = req["message_id"]

            st.subheader(f"Request {idx+1}/{len(requests)}")
            st.markdown(f"> {req['raw_body']}")
            st.write("🔍 Full request dict:", req)

            if "json_output" in req:
                try:
                    st.json(json.loads(req["json_output"]))
                except:
                    st.write(req["json_output"])

            fmt  = st.text_input("Container Format", value=req.get("container_format",""), key=f"fmt_{mid}")
            qty  = st.text_input("Quantity",         value=req.get("quantity",""),         key=f"qty_{mid}")
            date = st.text_input("Date Needed",      value=req.get("date_needed",""),      key=f"date_{mid}")
            user = st.text_input("Requester",        value=req.get("requester",""),        key=f"user_{mid}")

            c1, c2, c3 = st.columns(3)
            if c1.button("✅ Approve", key=f"app_{mid}"):
                mark_request_read(mid)
                st.success("Approved")
            if c2.button("❌ Reject", key=f"rej_{mid}"):
                mark_request_read(mid)
                st.warning("Rejected")
            if c3.button("➡️ Next", key=f"next_{mid}"):
                st.session_state.req_idx += 1

    # ── Tab 2: Chat + Maps
    with tab2:
        if "messages" not in st.session_state:
            st.session_state.messages = []

        # replay chat
        for msg in st.session_state.messages:
            who = "You" if msg["role"]=="user" else "Assistant"
            st.markdown(f"**{who}:** {msg['content']}")

        query = st.text_input("Your question:", key="chat_input")
        if st.button("Send", key="chat_send") and query:
            st.session_state.messages.append({"role":"user","content":query})

            # 1) address logic?
            if handle_address_logic(query, ""):
                return  # done

            # 2) else call Cortex agent
            events = snowflake_api_call(query)
            text, sql, citations = process_sse_response(events or [])

            # 3) if no agent output at all → plain completion
            if not text and not sql and not citations:
                text = direct_completion(query)

            # 4) show assistant text
            if text:
                st.session_state.messages.append({"role":"assistant","content":text})
                st.markdown(f"**Assistant:** {text}")

            # 5) show SQL + results
            if sql:
                st.markdown("### Generated SQL")
                st.code(sql, language="sql")
                df = run_snowflake_query(sql)
                if df is not None:
                    st.write("### Results")
                    st.dataframe(df)

            # 6) show citations
            if citations:
                st.write("Citations:")
                for c in citations:
                    lbl = c["source_id"] or "source"
                    q_sql = (
                        "SELECT transcript_text "
                        "FROM sales_conversations "
                        f"WHERE conversation_id = '{c['doc_id']}'"
                    )
                    df2 = run_snowflake_query(q_sql)
                    txt = "No transcript available"
                    if df2 is not None:
                        pdf = df2.to_pandas()
                        if not pdf.empty:
                            txt = pdf.iloc[0,0]
                    with st.expander(lbl):
                        st.write(txt)

    # ── Sidebar: reset chat
    with st.sidebar:
        if st.button("🔄 New Conversation", key="new_chat"):
            st.session_state.messages = []
            st.rerun()

if __name__ == "__main__":
    main()
