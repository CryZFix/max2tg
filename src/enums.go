package src

const (
	WEBSOCKET_URL = "wss://ws-api.oneme.ru/websocket"
)

type WebSocketPayload map[string]interface{}

type Opcode int32

const (
	PING           Opcode = 1
	INIT           Opcode = 6
	AUTH           Opcode = 19
	GET_CONTACTS   Opcode = 32
	GET_CHATS      Opcode = 48
	GET_MESSAGES   Opcode = 49
	SUBSCRIBE_CHAT Opcode = 75
	SEND_MESSAGE   Opcode = 64
	DELETE_MESSAGE Opcode = 66
	EDIT_MESSAGE   Opcode = 67
	GET_MESSAGE    Opcode = 71
	GET_VIDEO      Opcode = 83
	GET_FILE       Opcode = 88
	IMAGE_UPLOAD   Opcode = 80
	VIDEO_UPLOAD   Opcode = 82
	FILE_UPLOAD    Opcode = 87
	NOTIF_ATTACH   Opcode = 136
	ON_MESSAGE     Opcode = 128
	SET_REACTION   Opcode = 178
)

type MessageStatus int32

const (
	MessageStatusNORMAL  MessageStatus = 0
	MessageStatusEDITED  MessageStatus = 1
	MessageStatusREMOVED MessageStatus = 2
)

type AttachmentType string

const (
	AttachmentTypeControl AttachmentType = "control"
	AttachmentTypeAudio   AttachmentType = "audio"
	AttachmentTypeFile    AttachmentType = "file"
	AttachmentTypePhoto   AttachmentType = "photo"
	AttachmentTypeVideo   AttachmentType = "video"
)

type ChatType string

const (
	ChatTypeChat    ChatType = "chat"
	ChatTypeChannel ChatType = "channel"
	ChatTypeGroup   ChatType = "group"
)

type MessageType string

const (
	MessageTypeText    MessageType = "text"
	MessageTypeVoice   MessageType = "voice"
	MessageTypeSticker MessageType = "sticker"
	MessageTypeGif     MessageType = "gif"
)
