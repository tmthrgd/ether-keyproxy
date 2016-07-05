package main

import (
	"bytes"
	"flag"
	"log"
	"sync"

	msgpack "github.com/hashicorp/go-msgpack/codec"
	serf "github.com/hashicorp/serf/client"
)

const (
	nameLen = 16

	installKeyEvent    = "install-key"
	removeKeyEvent     = "remove-key"
	setDefaultKeyEvent = "set-default-key"
	wipeKeysEvent      = "wipe-keys"

	retrieveKeysQuery = "retrieve-keys"
)

var defaultKey *[]byte
var keys [][]byte
var keysMut sync.RWMutex

func main() {
	wanConf := &serf.Config{}
	lanConf := &serf.Config{}

	flag.StringVar(&wanConf.Addr, "wan-addr", "127.0.0.1:7374", "the address to connect to")
	flag.StringVar(&wanConf.AuthKey, "wan-auth", "", "the RPC auth key")
	flag.DurationVar(&wanConf.Timeout, "wan-timeout", 0, "the RPC timeout")

	flag.StringVar(&lanConf.Addr, "lan-addr", "127.0.0.1:7373", "the address to connect to")
	flag.StringVar(&lanConf.AuthKey, "lan-auth", "", "the RPC auth key")
	flag.DurationVar(&lanConf.Timeout, "lan-timeout", 0, "the RPC timeout")

	var eventKeyPrefix string
	flag.StringVar(&eventKeyPrefix, "prefix", "ether:", "the serf event prefix")

	flag.Parse()

	wanRPC, err := serf.ClientFromConfig(wanConf)
	if err != nil {
		panic(err)
	}

	lanRPC, err := serf.ClientFromConfig(lanConf)
	if err != nil {
		panic(err)
	}

	queryCh := make(chan map[string]interface{})

	go func() {
		var buf []byte

		for req := range queryCh {
			if req["Name"] != eventKeyPrefix+retrieveKeysQuery {
				continue
			}

			keysMut.RLock()
			log.Printf("%s%s: %d keys", eventKeyPrefix, retrieveKeysQuery, len(keys))

			enc := msgpack.NewEncoderBytes(&buf, &msgpack.MsgpackHandle{RawToString: true, WriteExt: true})

			var resp struct {
				Default []byte
				Keys    [][]byte
			}

			if defaultKey != nil {
				resp.Default = (*defaultKey)[:nameLen]
			}

			resp.Keys = keys

			if err := enc.Encode(resp); err != nil {
				panic(err)
			}

			id, ok := req["ID"].(uint64)

			if !ok {
				id = (uint64)(req["ID"].(int64))
			}

			if err := lanRPC.Respond(id, buf); err != nil {
				panic(err)
			}
			keysMut.RUnlock()
		}
	}()

	if _, err = lanRPC.Stream("query", queryCh); err != nil {
		panic(err)
	}

	userCh := make(chan map[string]interface{})

	go func() {
	evLoop:
		for ev := range userCh {
			name, ok0 := ev["Name"].(string)
			payload, ok1 := ev["Payload"].([]byte)
			coalesce, ok2 := ev["Coalesce"].(bool)

			if !ok0 || !ok1 || !ok2 {
				panic("invalid event")
			}

			if name[:len(eventKeyPrefix)] != eventKeyPrefix {
				continue
			}

			if err = lanRPC.UserEvent(name, payload, coalesce); err != nil {
				panic(err)
			}

			switch name[len(eventKeyPrefix):] {
			case installKeyEvent:
				if len(payload) <= nameLen {
					panic("invalid event payload")
				}

				log.Printf("%s %x", name, payload[:nameLen])

				keysMut.Lock()
				for _, key := range keys {
					if bytes.Equal(key[:nameLen], payload[:nameLen]) {
						log.Printf("already have key %x", payload[:nameLen])

						keysMut.Unlock()
						continue evLoop
					}
				}

				keys = append(keys, payload)
				keysMut.Unlock()
			case removeKeyEvent:
				if len(payload) != nameLen {
					panic("invalid event payload")
				}

				log.Printf("%s %x", name, payload[:nameLen])

				keysMut.Lock()
				for i, key := range keys {
					if bytes.Equal(key[:nameLen], payload) {
						// zero old key
						for i := range key {
							key[i] = 0
						}

						keys[i] = keys[len(keys)-1]
						keys[len(keys)-1] = nil
						keys = keys[:len(keys)-1]

						keysMut.Unlock()
						continue evLoop
					}
				}

				log.Printf("cannot remove key %x", payload[:nameLen])
				keysMut.Unlock()
			case setDefaultKeyEvent:
				if len(payload) != nameLen {
					panic("invalid event payload")
				}

				log.Printf("%s %x", name, payload[:nameLen])

				keysMut.Lock()
				defaultKey = nil

				for _, key := range keys {
					if bytes.Equal(key[:nameLen], payload) {
						defaultKey = &key
						break
					}
				}

				if defaultKey == nil {
					log.Printf("cannot set default key %x", payload[:nameLen])
				}

				keysMut.Unlock()
			case wipeKeysEvent:
				if len(payload) != 0 {
					log.Printf("invalid %s event payload", eventKeyPrefix+wipeKeysEvent)
				}

				log.Println(name)

				keysMut.Lock()
				for _, key := range keys {
					// zero old key
					for i := range key {
						key[i] = 0
					}
				}

				defaultKey = nil
				keys = nil
				keysMut.Unlock()
			default:
				continue
			}
		}
	}()

	if _, err = wanRPC.Stream("user", userCh); err != nil {
		panic(err)
	}

	respCh := make(chan serf.NodeResponse, 1)

	if err = wanRPC.Query(&serf.QueryParam{
		RequestAck: false,
		Name:       eventKeyPrefix + retrieveKeysQuery,
		RespCh:     respCh,
	}); err != nil {
		panic(err)
	}

	log.Printf("Query '%s%s' dispatched", eventKeyPrefix, retrieveKeysQuery)

	keysMut.Lock()
	resp := <-respCh

	var mh msgpack.MsgpackHandle
	dec := msgpack.NewDecoderBytes(resp.Payload, &mh)

	var body struct {
		Default []byte
		Keys    [][]byte
	}

	if err := dec.Decode(&body); err != nil {
		panic(err)
	}

	keyNames := make([][nameLen]byte, len(body.Keys))
	for i, key := range body.Keys {
		copy(keyNames[i][:], key[:nameLen])
	}

	log.Printf(`%s%s response from '%s':
	Default: %x
	Keys: %x
	Total Keys: %d
`, eventKeyPrefix, retrieveKeysQuery, resp.From, body.Default, keyNames, len(body.Keys))

	switch len(body.Default) {
	case nameLen:
		defaultKey = nil

		for _, key := range body.Keys {
			if bytes.Equal(key[:nameLen], body.Default) {
				defaultKey = &key
				break
			}
		}

		if defaultKey == nil {
			log.Printf("cannot set default key %x", body.Default)
		}
	case 0:
	default:
		panic("invalid default key size")
	}

	keys = body.Keys
	keysMut.Unlock()

	select {}
}
