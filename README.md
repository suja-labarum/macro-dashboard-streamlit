# Macro Dashboard Streamlit

Streamlit macro dashboard for market, liquidity, options, institutional-flow, and news monitoring.

## Run Locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Deploy To Streamlit Community Cloud

1. Push this folder to GitHub.
2. Go to [share.streamlit.io](https://share.streamlit.io).
3. Create a new app from the GitHub repository.
4. Set the main file path to `app.py`.
5. Add API keys in Streamlit secrets using `.streamlit/secrets.toml.example` as the template.

## Notes

- Some feeds work without API keys, but API-backed sections need secrets.
- The AI Macro Analysis section uses local Codex CLI on your Mac. It will not run on Streamlit Cloud unless the cloud runtime has Codex CLI available.
