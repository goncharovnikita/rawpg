package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"
)

type messageType byte

const (
	messageTypeAuthentication     messageType = 'R'
	messageTypeParameterStatus    messageType = 'S'
	messageTypeBackendKeyData     messageType = 'K'
	messageTypeReadyForQuery      messageType = 'Z'
	messageTypeError              messageType = 'E'
	messageTypeNotice             messageType = 'N'
	messageTypeQuery              messageType = 'Q'
	messageTypeRowDescription     messageType = 'T'
	messageTypeDataRow            messageType = 'D'
	messageTypeEmptyQueryResponse messageType = 'I'
	messageTypeCommandComplete    messageType = 'C'
)

func (m messageType) String() string {
	return string(m)
}

type outgoingMessage interface {
	Binary() []byte
}

type incomingMessage struct {
	Type messageType
	Size int
	Data []byte
}

type appConfig struct {
	dbHost string
	dbPort int
	dbName string
	dbUser string

	query string
}

func main() {
	logger := slog.New(slog.Default().Handler())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	if err := run(ctx, logger); err != nil {
		logger.With("err", err).Error("application run with error")
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := getConfig()
	if err != nil {
		return fmt.Errorf("getting app config: %w", err)
	}

	addr := net.JoinHostPort(cfg.dbHost, fmt.Sprint(cfg.dbPort))

	pgTCPConn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", addr, err)
	}

	connectedAddr := pgTCPConn.LocalAddr().String()

	logger.With("addr", connectedAddr).Info("connected to postgres via tcp")

	mreader := newMessageReader(logger, pgTCPConn)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer logger.Info("pg TCP conn closed")

		<-ctx.Done()

		if err := pgTCPConn.Close(); err != nil {
			logger.With("err", err).Error("could not close pg tcp connection")
		}
	}()

	err = doHandshake(ctx, logger, pgTCPConn, mreader, cfg.dbUser, cfg.dbName)
	if err != nil {
		return fmt.Errorf("could not send startup message: %w", err)
	}

	logger.Info("handshake done")

	err = doSyncQuery(ctx, logger, pgTCPConn, mreader, ";")
	if err != nil {
		return fmt.Errorf("could not execute sync query: %w", err)
	}

	logger.Info("ping done")

	err = doSyncQuery(ctx, logger, pgTCPConn, mreader, cfg.query)
	if err != nil {
		return fmt.Errorf("could not execute sync query: %w", err)
	}

	logger.Info("selecting users done")

	wg.Wait()

	logger.Info("main exit")

	return nil
}

func getConfig() (appConfig, error) {
	dbHostFlag := flag.String("host", "localhost", "db host")
	dbPortFlag := flag.Int("port", 5432, "db port")
	dbNameFlag := flag.String("name", "postgres", "db name")
	dbUserFlag := flag.String("user", "postgres", "db user")
	queryFlag := flag.String("query", "", "query to execute")

	flag.Parse()

	if queryFlag == nil || *queryFlag == "" {
		return appConfig{}, fmt.Errorf("provide query with --query flag")
	}

	return appConfig{
		dbHost: *dbHostFlag,
		dbPort: *dbPortFlag,
		dbName: *dbNameFlag,
		dbUser: *dbUserFlag,
		query:  *queryFlag,
	}, nil
}

func makeCStringBinary(v string) []byte {
	return append([]byte(v), '\000')
}

type StartupMessage struct {
	User     string
	Database string
}

func (m StartupMessage) Binary() []byte {
	msg := make([]byte, 4)

	msg = binary.BigEndian.AppendUint32(msg, uint32(196608))

	msg = append(msg, makeCStringBinary("user")...)
	msg = append(msg, makeCStringBinary(m.User)...)

	msg = append(msg, makeCStringBinary("database")...)
	msg = append(msg, makeCStringBinary(m.Database)...)

	msg = append(msg, makeCStringBinary("")...)

	binary.BigEndian.PutUint32(msg, uint32(len(msg)))

	return msg
}

type QueryMessage struct {
	Query string
}

func (m QueryMessage) Binary() []byte {
	msg := make([]byte, 5)

	msg[0] = byte(messageTypeQuery)
	msg = append(msg, makeCStringBinary(m.Query)...)

	binary.BigEndian.PutUint32(msg[1:], uint32(len(msg[1:])))

	return msg
}

func sendMessageToConn(conn net.Conn, msg outgoingMessage) error {
	written := 0
	data := msg.Binary()

	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))

	for written < len(data) {
		w, err := conn.Write(data[written:])
		if err != nil {
			return fmt.Errorf("writing to net.Conn: %w", err)
		}

		written += w
	}

	return nil
}

type messageReader struct {
	logger *slog.Logger

	conn net.Conn
}

func newMessageReader(logger *slog.Logger, conn net.Conn) *messageReader {
	return &messageReader{
		logger: logger,
		conn:   conn,
	}
}

func (m *messageReader) ReadMessage() (incomingMessage, error) {
	header := make([]byte, 5)

	if _, err := io.ReadFull(m.conn, header); err != nil {
		return incomingMessage{}, fmt.Errorf("reading message header: %w", err)
	}

	msgType := messageType(header[0])
	msgSize := int(binary.BigEndian.Uint32(header[1:]))
	msgBody := make([]byte, msgSize-4)

	if _, err := io.ReadFull(m.conn, msgBody); err != nil {
		return incomingMessage{}, fmt.Errorf("reading message data: %w", err)
	}

	m.logger.
		With(
			"msg_type", string(msgType),
			"msg_size", msgSize,
			"msg_body", string(msgBody),
		).
		Debug("got message")

	return incomingMessage{
		Type: msgType,
		Size: msgSize,
		Data: msgBody,
	}, nil
}

func parseString(data []byte) (string, []byte) {
	res := make([]byte, 0)

	for _, v := range data {
		if v == 0 {
			break
		}

		res = append(res, v)
	}

	return string(res), data[len(res)+1:]
}

func parseInt16(data []byte) (int16, []byte) {
	var value int16

	value |= int16(data[0]) << 8
	value |= int16(data[1])

	return value, data[2:]
}

func parseInt32(data []byte) (int32, []byte) {
	var value int32

	value |= int32(data[0]) << 24
	value |= int32(data[1]) << 16
	value |= int32(data[2]) << 8
	value |= int32(data[3])

	return value, data[4:]
}

func parseBytes(data []byte, count int) ([]byte, []byte) {
	res := make([]byte, count)

	for i := 0; i < count; i++ {
		res[i] = data[i]
	}

	return res, data[count:]
}

func doHandshake(
	ctx context.Context,
	logger *slog.Logger,
	conn net.Conn,
	mreader *messageReader,
	user string,
	db string,
) error {
	l := logger.With("system", "handshake")

	err := sendMessageToConn(conn, &StartupMessage{
		User:     user,
		Database: db,
	})
	if err != nil {
		return fmt.Errorf("sending startup message: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("handshake cancelled")
	default:
	}

	msg, err := mreader.ReadMessage()
	if err != nil {
		return fmt.Errorf("reading message: %w", err)
	}

	if msg.Type != messageTypeAuthentication {
		return fmt.Errorf("unexpected response mesage type at handshake: %s", msg.Type)
	}

	if msg.Size != 8 {
		return fmt.Errorf("unsupported authentication method with data size != 8")
	}

	if binary.BigEndian.Uint32(msg.Data) != 0 {
		return fmt.Errorf("authentication is not supported")
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("handshake cancelled")
		default:
		}

		msg, err = mreader.ReadMessage()
		if err != nil {
			return fmt.Errorf("reading message: %w", err)
		}

		switch msg.Type {
		case messageTypeReadyForQuery:
			{
				l.Info("auth ok")

				return nil
			}

		case messageTypeParameterStatus:
			{
				k, rest := parseString(msg.Data)
				v, _ := parseString(rest)

				l.With("param_k", k, "param_v", v).Info("got type parameter")
			}

		case messageTypeBackendKeyData:
			l.Info("got backend key data, skipping")

		default:
			return fmt.Errorf("unsupported backend message: %s", msg.Type)
		}
	}
}

func doSyncQuery(
	ctx context.Context,
	logger *slog.Logger,
	conn net.Conn,
	mreader *messageReader,
	query string,
) error {
	l := logger.With("system", "sync_query")
	defer l.With("query", query).Info("query completed")

	err := sendMessageToConn(conn, QueryMessage{
		Query: query,
	})
	if err != nil {
		return fmt.Errorf("sending query message: %w", err)
	}

	for {
		l.Info("awaiting for response")

		select {
		case <-ctx.Done():
			return nil
		default:
		}

		resp, err := mreader.ReadMessage()
		if err != nil {
			return fmt.Errorf("reading message: %w", err)
		}

		switch resp.Type {
		case messageTypeReadyForQuery:
			l.Info("query completed successfully")

			return nil

		case messageTypeEmptyQueryResponse:
			l.Info("got empty response for query")

		case messageTypeCommandComplete:
			l.Info("command complete")

		case messageTypeRowDescription:
			{
				fieldsNum, rest := parseInt16(resp.Data)

				l.With("fields_num", fieldsNum).Info("fields num")

				for i := 0; i < int(fieldsNum); i++ {
					var (
						fName       string
						fObID       int32
						fAttrNum    int16
						fDtypeObID  int32
						fDtSize     int16
						fDtMod      int32
						fFormatCode int16
					)

					fName, rest = parseString(rest)
					fObID, rest = parseInt32(rest)
					fAttrNum, rest = parseInt16(rest)
					fDtypeObID, rest = parseInt32(rest)
					fDtSize, rest = parseInt16(rest)
					fDtMod, rest = parseInt32(rest)
					fFormatCode, rest = parseInt16(rest)

					logger.With(
						"name", fName,
						"object_id", fObID,
						"attr_num", fAttrNum,
						"data_type_object_id", fDtypeObID,
						"data_type_size", fDtSize,
						"data_type_mod", fDtMod,
						"format_code", fFormatCode,
					).Info("row description")
				}
			}

		case messageTypeDataRow:
			{
				fieldsNum, rest := parseInt16(resp.Data)

				l.With("fields_num", fieldsNum).Info("fields num")

				for i := 0; i < int(fieldsNum); i++ {
					var (
						fSize int32
						fData []byte
					)

					fSize, rest = parseInt32(rest)

					if fSize == -1 {
						fData = []byte("NULL")
					} else {
						fData, rest = parseBytes(rest, int(fSize))
					}

					logger.With(
						"data_size", fSize,
						"data", string(fData),
					).Info("row")
				}
			}

		default:
			return fmt.Errorf("unsupported message type during query: %s", resp.Type)
		}
	}
}
