package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
)

type TransferSession struct {
	conn           net.Conn
	file           *TransferFile
	lastBlockIndex int64
	server         *Server
}

// one file is corresponding to multiple sessions(default: 8)
type TransferFile struct {
	metadata *FileMetadata
	file     *os.File
	sessions map[*TransferSession]*TransferSession
	hasher   hash.Hash
	digest   string
	closed   bool
}

// one server could receive multiple files simultaneously
type Server struct {
	params         *ServerParams
	listener       *net.TCPListener
	totalSize      int64
	lastBlockIndex int64
	receivedBlocks int64
	files          map[string]*TransferFile
	sessions       []*TransferSession
}

type ServerParams struct {
	ip   string
	port int
}

func md5sum(filepath string) (string, error) {
	fin, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	hasher := md5.New()
	io.Copy(hasher, fin)
	digest := hex.EncodeToString(hasher.Sum(nil))
	fin.Close()
	return digest, nil
}

func NewServerParams() *ServerParams {
	return new(ServerParams)
}

func (session *TransferSession) handleFileMetadata(metadata *FileMetadata) {
	file, ok := session.server.files[metadata.name]
	fmt.Printf("handleFileMetadata %s %v %v %v\n", metadata.name, metadata.totalSize, file, ok)
	if !ok {
		file = new(TransferFile)
		file.metadata = metadata
		file.sessions = make(map[*TransferSession]*TransferSession)
		file.hasher = md5.New()
		file.closed = false
		var err error
		file.file, err = os.OpenFile(metadata.name, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			fmt.Printf("handleFileMetadata: failed to open file %s %v\n", metadata.name, err)
			return
		}
		fmt.Printf("handleFileMetadata: open file ok: %s %v\n", metadata.name, file.file)
		//pos, err := file.file.Seek(metadata.totalSize-1, os.SEEK_SET)
		//fmt.Printf("handleFileMetadata: open file ok: %s %v pos=%v err=%v\n", metadata.name, file.file, pos, err)
		//size, err := file.file.WriteAt([]byte{0}, metadata.totalSize-1)
		err = file.file.Truncate(metadata.totalSize)
		fmt.Printf("handleFileMetadata: write file: %s %v err=%v\n", metadata.name, file.file, err)
		if err != nil {
			fmt.Printf("handleFileMetadata: failed to open file %s %v\n", metadata.name, err)
			return
		}
		session.server.files[metadata.name] = file
	}
	file.sessions[session] = session
	session.file = file
}

func (session *TransferSession) handleBlock(block *BlockInfo) {
	fmt.Printf("handleBlock %v %v\n", block.size, block.blockIndex)
	offset := int64(session.file.metadata.blockSize) * block.blockIndex
	size, err := session.file.file.WriteAt(block.GetData(), offset)
	fmt.Printf("handleBlock WriteAt %v %v pos=%v err=%v\n", block.blockIndex, block.size, size, err)
}

func (session *TransferSession) closeSession(err error) {
	fmt.Printf("closeSession %v\n", err)
	delete(session.file.sessions, session)
	if len(session.file.sessions) == 0 && !session.file.closed {
		fmt.Printf("closeSession: all sessions are closed, file is finished.\n")
		session.file.file.Close()
		session.file.closed = true
		delete(session.server.files, session.file.metadata.name)
		fmt.Printf("closeSession: close file.\n")
		digest, err := md5sum(session.file.metadata.name)
		if err != nil || digest != session.file.digest {
			fmt.Printf("closeSession: invalid digest: sender=%v reciver=%v\n", session.file.digest, digest)
		} else {
			fmt.Printf("closeSession: digest ok: sender=%v reciver=%v\n", session.file.digest, digest)
		}
	}
}

func (server *Server) serveOne(conn net.Conn) {
	defer conn.Close()
	session := new(TransferSession)
	session.conn = conn
	session.server = server
	packetType := []byte{0}
	data := make([]byte, 512*1024)
	var totalSize int64 = 0
	block := new(BlockInfo)
	block.buffer = data
	fe := new(FileEOF)
	metadata := new(FileMetadata)
	for {
		size, err := conn.Read(packetType)
		if err != nil {
			fmt.Printf("serveOne failed to read packet type: %d %v %v\n", size, packetType[0], err)
			session.closeSession(err)
			return
		}
		fmt.Printf("serveOne read packet type ok: %d %v %v\n", size, packetType[0], err)
		if packetType[0] == FILE_METADATA_PACKET {
			err = metadata.Read(conn)
			if err != nil {
				fmt.Println("failed to receive and decode filemetadata packet:", err.Error())
				break
			}
			fmt.Printf("serveOne: file metadata packet: %s %v %v\n", metadata.name, metadata.totalSize, metadata.blockSize)
			session.handleFileMetadata(metadata)
		} else if packetType[0] == BLOCK_PACKET {
			err = block.Read(conn)
			if err != nil {
				fmt.Println("failed to receive and decode block packet:", err.Error())
				break
			}
			fmt.Printf("serveOne: block packet: %v %v %v\n", block.blockIndex, block.size, len(block.buffer))
			totalSize += int64(block.size)
			fmt.Printf("total received size=%d\n", totalSize)
			session.handleBlock(block)
		} else if packetType[0] == FILE_EOF_PACKET {
			err = fe.Read(conn)
			if err != nil {
				fmt.Println("failed to receive and decode eof packet:", err.Error())
				break
			}
			session.file.digest = hex.EncodeToString(fe.digest)
			fmt.Printf("serveOne: eof packet: digest=%s\n", session.file.digest)
			totalSize += int64(block.size)
			fmt.Printf("total received size=%d\n", totalSize)
			session.handleBlock(block)
		} else {
			fmt.Printf("ERROR: serveOne: unknown packet: %d------------------------------\n", packetType[0])
			break
		}
	}
}

func (server *Server) ServeForever() {
	for {
		conn, err := server.listener.AcceptTCP()
		if err != nil {
			fmt.Println("failed to accept connection:", err.Error())
			continue
		}
		fmt.Println("accept connection:", conn.RemoteAddr().String())
		go server.serveOne(conn)
	}
}

func (server *Server) Run(params *ServerParams) {
	var err error
	server.params = params
	server.listener, err = net.ListenTCP("tcp", &net.TCPAddr{net.ParseIP(params.ip), params.port, ""})
	if err != nil {
		fmt.Printf("failed to bind ip:%s port:%d, error:%s\n", params.ip, params.port, err.Error())
		return
	}
	fmt.Printf("server is started, waiting for connections. ip:%s port:%d\n", params.ip, params.port)
	server.ServeForever()
}

func NewServer() *Server {
	server := new(Server)
	server.files = make(map[string]*TransferFile)
	return server
}
