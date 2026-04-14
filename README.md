# Discord Outreach Engine (D.O.E.)

Multi-token client management and outreach automation with AI-powered chat and vision.

## Features
- Multi-token support with proxy rotation
- AI Response Generation via Alibaba DashScope (Qwen-Plus)
- Vision Integration for describing attachments (Qwen-VL-Plus)
- SQLite backend for message history and member profiles
- Flask-based Web GUI for monitoring and approving suggestions
- Context compression and persona management

## Setup

1. **Clone & Install**
   ```bash
   git clone https://github.com/yanneksevinc/discord-outreach-engine.git
   cd discord-outreach-engine
   pip install -r requirements.txt
   ```

2. **Configuration**
   - Create a `.env` file for sensitive keys:
     ```env
     DASHSCOPE_API_KEY=your_key_here
     DOE_DB_PATH=doe.db
     FLASK_SECRET_KEY=some_random_string
     ```
   - Edit `config.json` for proxies and discord credentials.

3. **Database**
   The system will automatically initialize `doe.db` on first run.

4. **Scraper**
   Ensure your `scraper.py` is present in the root directory.

## Running the Engine

Start both the worker pool and the dashboard:
```bash
python main.py
```

Access the dashboard in your browser at `http://your-vps-ip:8000`.

## Project Structure
- `main.py`: Entry point
- `gui.py`: Flask Dashboard
- `ai_engine.py`: DashScope & Persona logic
- `vision_engine.py`: Image interpretation
- `config.py`: Configuration handler
- `login_manager.py`: Token & Session management
- `sync_utils.py`: Database helpers
- `worker.py`: Message processing loop