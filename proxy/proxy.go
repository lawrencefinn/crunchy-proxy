/*
Copyright 2017 Crunchy Data Solutions, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/crunchydata/crunchy-proxy/common"
	"github.com/crunchydata/crunchy-proxy/config"
	"github.com/crunchydata/crunchy-proxy/connect"
	"github.com/crunchydata/crunchy-proxy/pool"
	"github.com/crunchydata/crunchy-proxy/protocol"
	"github.com/crunchydata/crunchy-proxy/util/log"
)

type Proxy struct {
	writePools chan *pool.Pool
	readPools  chan *pool.Pool
	master     common.Node
	clients    []net.Conn
	Stats      map[string]int32
	lock       *sync.Mutex
}

func NewProxy() *Proxy {
	p := &Proxy{
		Stats: make(map[string]int32),
		lock:  &sync.Mutex{},
	}

	p.setupPools()

	return p
}

func (p *Proxy) setupPools() {
	nodes := config.GetNodes()
	capacity := config.GetPoolCapacity()

	/* Initialize pool structures */
	numNodes := len(nodes)
	p.writePools = make(chan *pool.Pool, numNodes)
	p.readPools = make(chan *pool.Pool, numNodes)

	for name, node := range nodes {
		/* Create Pool for Node */
		newPool := pool.NewPool(name, capacity)

		if node.Role == common.NODE_ROLE_MASTER {
			p.writePools <- newPool
		} else {
			p.readPools <- newPool
		}

		/* Create connections and add to pool. */
		for i := 0; i < capacity; i++ {
			/* Connect and authenticate */
			log.Infof("Connecting to node '%s' at %s...", name, node.HostPort)
			connection, err := connect.Connect(node.HostPort)

			username := config.GetString("credentials.username")
			database := config.GetString("credentials.database")
			options := config.GetStringMapString("credentials.options")

			startupMessage := protocol.CreateStartupMessage(username, database, options)

			connection.Write(startupMessage)

			response := make([]byte, 4096)
			connection.Read(response)

			authenticated := connect.HandleAuthenticationRequest(connection, response)

			if !authenticated {
				log.Error("Authentication failed")
			}

			if err != nil {
				log.Errorf("Error establishing connection to node '%s'", name)
				log.Errorf("Error: %s", err.Error())
			} else {
				log.Infof("Successfully connected to '%s' at '%s'", name, node.HostPort)
				newPool.Add(connection)
			}
		}
	}
}

// Get the next pool. If read is set to true, then a 'read-only' pool will be
// returned. Otherwise, a 'read-write' pool will be returned.
func (p *Proxy) getPool(read bool) *pool.Pool {
	if read {
		return <-p.readPools
	}
	return <-p.writePools
}

// Return the pool. If read is 'true' then, the pool will be returned to the
// 'read-only' collection of pools. Otherwise, it will be returned to the
// 'read-write' collection of pools.
func (p *Proxy) returnPool(pl *pool.Pool, read bool) {
	if read {
		p.readPools <- pl
	} else {
		p.writePools <- pl
	}
}

// HandleConnection handle an incoming connection to the proxy
func (p *Proxy) HandleConnection(client net.Conn) {
	/* Get the client startup message. */
	message, length, err := connect.Receive(client)

	if err != nil {
		log.Error("Error receiving startup message from client.")
		log.Errorf("Error: %s", err.Error())
	}

	log.Infof("Handling connection %s", string(message))

	/* Get the protocol from the startup message.*/
	version := protocol.GetVersion(message)

	/* Handle the case where the startup message was an SSL request. */
	log.Infof("Version %s",  version)
	log.Infof("Message %v length %d", message, length)
	if version == protocol.SSLRequestCode {
		sslResponse := protocol.NewMessageBuffer([]byte{})

		/* Determine which SSL response to send to client. */
		creds := config.GetCredentials()
		if creds.SSL.Enable {
			sslResponse.WriteByte(protocol.SSLAllowed)
		} else {
			log.Info("Sending no ssl")
			sslResponse.WriteByte(protocol.SSLNotAllowed)
		}

		/*
		 * Send the SSL response back to the client and wait for it to send the
		 * regular startup packet.
		 */
		connect.Send(client, sslResponse.Bytes())

		/* Upgrade the client connection if required. */
		client = connect.UpgradeServerConnection(client)

		/*
		 * Re-read the startup message from the client. It is possible that the
		 * client might not like the response given and as a result it might
		 * close the connection. This is not an 'error' condition as this is an
		 * expected behavior from a client.
		 */
		if message, length, err = connect.Receive(client); err == io.EOF {
			log.Info("The client closed the connection.")
			return
		}
		if length == 0 {
			messageToSend := make([]byte, 9)
			messageToSend[0] = 'R'
			var someLength uint32
			someLength = 8
			var someMessage uint32
			someMessage = 0
			binary.BigEndian.PutUint32(messageToSend[1:5], someLength)
			binary.BigEndian.PutUint32(messageToSend[5:9], someMessage)
			fmt.Printf("Binary %v", messageToSend)
			connect.Send(client, messageToSend)
		}
		if message, length, err = connect.Receive(client); err == io.EOF {
			log.Info("The client closed the connection.")
			return
		}
		log.Infof("Message %s length %d", message, length)
	}

	/*
	 * Validate that the client username and database are the same as that
	 * which is configured for the proxy connections.
	 *
	 * If the the client cannot be validated then send an appropriate PG error
	 * message back to the client.
	 */
	if !connect.ValidateClient(message) {
		pgError := protocol.Error{
			Severity: protocol.ErrorSeverityFatal,
			Code:     protocol.ErrorCodeInvalidAuthorizationSpecification,
			Message:  "could not validate user/database",
		}

		connect.Send(client, pgError.GetMessage())
		log.Errorf("Could not validate client %s", pgError.GetMessage())
		return
	}

	/* Authenticate the client against the appropriate backend. */
	log.Infof("Client: %s - authenticating", client.RemoteAddr())
	//authenticated, err := connect.AuthenticateClient(client, message, length)
	//log.Infof("Authenticated? %v", authenticated)
	messageToSend := make([]byte, 13)
	messageToSend[0] = 'R'
	var someLength uint32
	someLength = 12
	var someMessage uint32
	someMessage = 5
	binary.BigEndian.PutUint32(messageToSend[1:5], someLength)
	binary.BigEndian.PutUint32(messageToSend[5:9], someMessage)
	messageToSend[9] = byte(81)
	messageToSend[10] = byte(31)
	messageToSend[11] = byte(191)
	messageToSend[12] = byte(4)
	fmt.Printf("Binary %v", messageToSend)
	_, err = connect.Send(client, messageToSend)
	message, length, err = connect.Receive(client)
	fmt.Printf("Another response %s %v", message, message)

	okMessage := make([]byte, 9)
	okMessage[0] = 'R'
	someLength = 8
	someMessage = 0
	binary.BigEndian.PutUint32(okMessage[1:5], someLength)
	binary.BigEndian.PutUint32(okMessage[5:9], someMessage)
	fmt.Printf("Sending %s %v\n", okMessage, okMessage)
	_, err = connect.Send(client, okMessage)
	//message, length, err = connect.Receive(client)
	//fmt.Printf("Another response %s %v\n", message, message)
	//fmt.Printf("WTF\n")

	okMessage = make([]byte, 6)
	okMessage[0] = 'Z'
	someLength = 5
	binary.BigEndian.PutUint32(okMessage[1:5], someLength)
	okMessage[5] = 'I'
	fmt.Printf("Sending %s %v\n", okMessage, okMessage)
	_, err = connect.Send(client, okMessage)
	message, length, err = connect.Receive(client)
	fmt.Printf("ZI response %s %v\n", message, message)

	if message[0] == 'P' {
		queryLength := binary.BigEndian.Uint32(message[1:5])
		buffer := protocol.NewMessageBuffer(message)
		buffer.Seek(5)
		preparedStmtName, _ := buffer.ReadString()
		//buffer.Seek(5 + len(preparedStmtName))
		query, _ := buffer.ReadString()
		numParams, _ := buffer.ReadInt16()
		fmt.Printf("Looks like a parse query of length %d \"%s\" \"%s\" %d\n", queryLength, preparedStmtName, query,  numParams)
		bindByte, _ := buffer.ReadByte()
		if bindByte == 'B' {
			bindLength, _ := buffer.ReadInt32()
			fmt.Printf("Bind byte? %s %d\n", string(bindByte), bindLength)
			buffer.ReadBytes(int(bindLength - 4))
		}
		describeByte, _ := buffer.ReadByte()
		if describeByte == 'D' {
			dataLength, _ := buffer.ReadInt32()
			describeType, _ := buffer.ReadByte()
			describeName, _ := buffer.ReadString()
			fmt.Printf("Data byte? %s %d %s \"%s\"\n", string(describeByte), dataLength, string(describeType), describeName)
		}
		executeByte, _ := buffer.ReadByte()
		if executeByte == 'E' {
			executeLength, _ := buffer.ReadInt32()
			executeName, _ := buffer.ReadString()
			maxRows, _ := buffer.ReadInt32()
			fmt.Printf("Execute byte %s %d \"%s\" %d\n", string(executeByte), executeLength, executeName, maxRows)
		}
		flushByte, _ := buffer.ReadByte()
		if flushByte == 'H' {
			flushLength, _ := buffer.ReadInt32()
			fmt.Printf("Flush byte %s %d\n", string(flushByte), flushLength)
		}
		syncByte, _ := buffer.ReadByte()
		if syncByte == 'S' {
			syncLength, _ := buffer.ReadInt32()
			fmt.Printf("Sync byte %s %d\n", string(syncByte), syncLength)
		}
	}

	noMessage := make([]byte, 5)
	noMessage[0] = 'n'
	someLength = 4
	binary.BigEndian.PutUint32(noMessage[1:5], someLength)

	parseMessage := make([]byte, 5)
	parseMessage[0] = '1'
	someLength = 4
	binary.BigEndian.PutUint32(parseMessage[1:5], someLength)
	bindMessage := make([]byte, 5)
	bindMessage[0] = '2'
	someLength = 4
	binary.BigEndian.PutUint32(bindMessage[1:5], someLength)
	fmt.Printf("Sending %s %v\n", parseMessage, parseMessage)
	_, err = connect.Send(client, parseMessage)
	_, err = connect.Send(client, bindMessage)
	_, err = connect.Send(client, noMessage)
	_, err = connect.Send(client, okMessage)
	message, length, err = connect.Receive(client)
	fmt.Printf("ZI response %s %v", message, message)

	authenticated := true
	/* If the client could not authenticate then go no further. */
	if err == io.EOF {
		log.Error("Bleh eof")
		return
	} else if !authenticated {
		log.Errorf("Client: %s - authentication failed", client.RemoteAddr())
		log.Errorf("Error: %s", err.Error())
		return
	} else {
		log.Infof("Client: %s - authentication successful", client.RemoteAddr())
	}

	/* Process the client messages for the life of the connection. */
	var statementBlock bool
	var cp *pool.Pool    // The connection pool in use
	var backend net.Conn // The backend connection in use
	var read bool
	var end bool
	var nodeName string

	for {
		var done bool // for message processing loop.

		message, length, err = connect.Receive(client)

		if err != nil {
			switch err {
			case io.EOF:
				log.Infof("Client: %s - closed the connection", client.RemoteAddr())
			default:
				log.Errorf("Error reading from client connection %s", client.RemoteAddr())
				log.Errorf("Error: %s", err.Error())
			}
			break
		}

		messageType := protocol.GetMessageType(message)

		/*
		 * If the message is a simple query, then it can have read/write
		 * annotations attached to it. Therefore, we need to process it and
		 * determine which backend we need to send it to.
		 */
		if messageType == protocol.TerminateMessageType {
			log.Infof("Client: %s - disconnected", client.RemoteAddr())
			return
		} else if messageType == protocol.QueryMessageType {
			annotations := getAnnotations(message)

			if annotations[StartAnnotation] {
				statementBlock = true
			} else if annotations[EndAnnotation] {
				end = true
				statementBlock = false
			}

			read = annotations[ReadAnnotation]

			/*
			 * If not in a statement block or if the pool or backend are not already
			 * set, then fetch a new backend to receive the message.
			 */
			if !statementBlock && !end || cp == nil || backend == nil {
				cp = p.getPool(read)
				backend = cp.Next()
				nodeName = cp.Name
				p.returnPool(cp, read)
			}

			/* Update the query count for the node being used. */
			p.lock.Lock()
			p.Stats[nodeName] += 1
			p.lock.Unlock()

			/* Relay message to client and backend */
			if _, err = connect.Send(backend, message[:length]); err != nil {
				log.Debugf("Error sending message to backend %s", backend.RemoteAddr())
				log.Debugf("Error: %s", err.Error())
			}

			/*
			 * Continue to read from the backend until a 'ReadyForQuery' message is
			 * is found.
			 */
			for !done {
				if message, length, err = connect.Receive(backend); err != nil {
					log.Debugf("Error receiving response from backend %s", backend.RemoteAddr())
					log.Debugf("Error: %s", err.Error())
					done = true
				}

				messageType := protocol.GetMessageType(message[:length])

				/*
				 * Examine all of the messages in the buffer and determine if any of
				 * them are a ReadyForQuery message.
				 */
				for start := 0; start < length; {
					messageType = protocol.GetMessageType(message[start:])
					messageLength := protocol.GetMessageLength(message[start:])

					/*
					 * Calculate the next start position, add '1' to the message
					 * length to account for the message type.
					 */
					start = (start + int(messageLength) + 1)
				}

				if _, err = connect.Send(client, message[:length]); err != nil {
					log.Debugf("Error sending response to client %s", client.RemoteAddr())
					log.Debugf("Error: %s", err.Error())
					done = true
				}

				done = (messageType == protocol.ReadyForQueryMessageType)
			}

			/*
			 * If at the end of a statement block or not part of statment block,
			 * then return the connection to the pool.
			 */
			if !statementBlock {
				/*
				 * Toggle 'end' such that a new connection will be fetched on the
				 * next query.
				 */
				if end {
					end = false
				}

				/* Return the backend to the pool it belongs to. */
				cp.Return(backend)
			}
		}
	}
}
