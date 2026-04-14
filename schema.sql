PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    platform TEXT NOT NULL,
    username TEXT,
    email TEXT,
    password_hash TEXT,
    proxy_id INTEGER,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (proxy_id) REFERENCES proxies(id)
        ON UPDATE CASCADE
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS proxies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host TEXT NOT NULL,
    port INTEGER NOT NULL,
    protocol TEXT NOT NULL DEFAULT 'http',
    username TEXT,
    password TEXT,
    label TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(host, port, protocol)
);

CREATE TABLE IF NOT EXISTS user_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    platform_user_id TEXT NOT NULL,
    username TEXT,
    display_name TEXT,
    bio TEXT,
    location TEXT,
    language TEXT,
    timezone TEXT,
    profile_url TEXT,
    metadata_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES accounts(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    UNIQUE(account_id, platform_user_id)
);

CREATE TABLE IF NOT EXISTS interests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_profile_id INTEGER NOT NULL,
    interest_type TEXT NOT NULL,
    value TEXT NOT NULL,
    weight REAL NOT NULL DEFAULT 1.0,
    source TEXT,
    metadata_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_profile_id) REFERENCES user_profiles(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    UNIQUE(user_profile_id, interest_type, value)
);

CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    user_profile_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    topic TEXT,
    last_message_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (account_id) REFERENCES accounts(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    FOREIGN KEY (user_profile_id) REFERENCES user_profiles(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('inbound', 'outbound')),
    external_message_id TEXT,
    content TEXT NOT NULL,
    metadata_json TEXT,
    sent_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    UNIQUE(conversation_id, external_message_id)
);

CREATE TABLE IF NOT EXISTS context_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL UNIQUE,
    summary TEXT NOT NULL,
    key_facts_json TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id)
        ON UPDATE CASCADE
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_account_id ON user_profiles(account_id);
CREATE INDEX IF NOT EXISTS idx_interests_user_profile_id ON interests(user_profile_id);
CREATE INDEX IF NOT EXISTS idx_conversations_account_id ON conversations(account_id);
CREATE INDEX IF NOT EXISTS idx_conversations_user_profile_id ON conversations(user_profile_id);
CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_sent_at ON messages(sent_at);
