package src

import (
	"database/sql"

	_ "modernc.org/sqlite"
)

type Database struct {
	db *sql.DB
}

func NewDatabase(dbPath string) (*Database, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	if err := createTables(db); err != nil {
		return nil, err
	}

	return &Database{db: db}, nil
}

func createTables(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			max_message_id INTEGER UNIQUE,
			tg_message_id INTEGER,
			max_sender_id INTEGER,
			timestamp INTEGER,
			edited_at INTEGER,
			max_chat_id INTEGER DEFAULT 0
		);

		CREATE TABLE IF NOT EXISTS topic_cache (
			max_chat_id INTEGER PRIMARY KEY,
			telegram_topic_id INTEGER NOT NULL
		);

		CREATE TABLE IF NOT EXISTS telegram_message_links (
			tg_message_id INTEGER PRIMARY KEY,
			max_message_id INTEGER NOT NULL,
			max_chat_id INTEGER NOT NULL
		);
	`)
	if err != nil {
		return err
	}
	db.Exec(`ALTER TABLE messages ADD COLUMN max_chat_id INTEGER DEFAULT 0`)
	return nil
}

func (d *Database) AddTelegramMessageLink(tgMessageID, maxMessageID int64, maxChatID int) error {
	_, err := d.db.Exec(`INSERT OR REPLACE INTO telegram_message_links (tg_message_id, max_message_id, max_chat_id) VALUES (?, ?, ?)`, tgMessageID, maxMessageID, maxChatID)
	return err
}

func (d *Database) GetMaxMessageForTgID(tgMessageID int64) (int64, int, bool, error) {
	if record, err := d.GetMessageByTgID(tgMessageID); err != nil || record != nil {
		if err != nil || record == nil {
			return 0, 0, false, err
		}
		return record["max_message_id"].(int64), int(record["max_chat_id"].(int64)), true, nil
	}
	var maxID int64
	var chatID int
	err := d.db.QueryRow(`SELECT max_message_id, max_chat_id FROM telegram_message_links WHERE tg_message_id = ?`, tgMessageID).Scan(&maxID, &chatID)
	if err == sql.ErrNoRows {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, err
	}
	return maxID, chatID, true, nil
}

func (d *Database) AddMessage(maxMessageID, tgMessageID, maxSenderID, timestamp, editedAt int64, maxChatID ...int) error {
	chatID := 0
	if len(maxChatID) > 0 {
		chatID = maxChatID[0]
	}
	_, err := d.db.Exec(
		`INSERT OR REPLACE INTO messages (max_message_id, tg_message_id, max_sender_id, timestamp, edited_at, max_chat_id) VALUES (?, ?, ?, ?, ?, ?)`,
		maxMessageID, tgMessageID, maxSenderID, timestamp, editedAt, chatID,
	)
	return err
}

func (d *Database) GetMessageByMaxID(maxMessageID int64) (map[string]interface{}, error) {
	row := d.db.QueryRow(
		`SELECT id, max_message_id, tg_message_id, max_sender_id, timestamp, edited_at, max_chat_id FROM messages WHERE max_message_id = ?`,
		maxMessageID,
	)

	var id, maxMsgID, tgMsgID, senderID, ts, editedAt, chatID int64
	err := row.Scan(&id, &maxMsgID, &tgMsgID, &senderID, &ts, &editedAt, &chatID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return map[string]interface{}{
		"id":             id,
		"max_message_id": maxMsgID,
		"tg_message_id":  tgMsgID,
		"max_sender_id":  senderID,
		"timestamp":      ts,
		"edited_at":      editedAt,
		"max_chat_id":    chatID,
	}, nil
}

func (d *Database) GetMessageByTgID(tgMessageID int64) (map[string]interface{}, error) {
	row := d.db.QueryRow(
		`SELECT id, max_message_id, tg_message_id, max_sender_id, timestamp, edited_at, max_chat_id FROM messages WHERE tg_message_id = ?`,
		tgMessageID,
	)

	var id, maxMsgID, tgMsgID, senderID, ts, editedAt, chatID int64
	err := row.Scan(&id, &maxMsgID, &tgMsgID, &senderID, &ts, &editedAt, &chatID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	return map[string]interface{}{
		"id":             id,
		"max_message_id": maxMsgID,
		"tg_message_id":  tgMsgID,
		"max_sender_id":  senderID,
		"timestamp":      ts,
		"edited_at":      editedAt,
		"max_chat_id":    chatID,
	}, nil
}

func (d *Database) DeleteMessageByMaxID(maxMessageID int64) error {
	_, err := d.db.Exec(`DELETE FROM messages WHERE max_message_id = ?`, maxMessageID)
	return err
}

func (d *Database) UpdateMessageEditedAt(maxMessageID, editedAt int64) error {
	_, err := d.db.Exec(
		`UPDATE messages SET edited_at = ? WHERE max_message_id = ?`,
		editedAt, maxMessageID,
	)
	return err
}

func (d *Database) GetAllMessages() ([]map[string]interface{}, error) {
	rows, err := d.db.Query(
		`SELECT id, max_message_id, tg_message_id, max_sender_id, timestamp, edited_at, max_chat_id FROM messages`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id, maxMsgID, tgMsgID, senderID, ts, editedAt, chatID int64
		if err := rows.Scan(&id, &maxMsgID, &tgMsgID, &senderID, &ts, &editedAt, &chatID); err != nil {
			return nil, err
		}
		results = append(results, map[string]interface{}{
			"id":             id,
			"max_message_id": maxMsgID,
			"tg_message_id":  tgMsgID,
			"max_sender_id":  senderID,
			"timestamp":      ts,
			"edited_at":      editedAt,
			"max_chat_id":    chatID,
		})
	}

	return results, nil
}

func (d *Database) GetCachedTopicID(maxChatID int) (int, error) {
	row := d.db.QueryRow(`SELECT telegram_topic_id FROM topic_cache WHERE max_chat_id = ?`, maxChatID)
	var topicID int
	err := row.Scan(&topicID)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return topicID, nil
}

func (d *Database) CacheTopicID(maxChatID, topicID int) error {
	_, err := d.db.Exec(
		`INSERT OR REPLACE INTO topic_cache (max_chat_id, telegram_topic_id) VALUES (?, ?)`,
		maxChatID, topicID,
	)
	return err
}

func (d *Database) GetAllCachedChatIDs() ([]int, error) {
	rows, err := d.db.Query(`SELECT max_chat_id FROM topic_cache`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (d *Database) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}
