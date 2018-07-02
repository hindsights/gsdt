package main

import (
	"container/list"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ClientParams struct {
	address     string
	clientCount int
	path        string
	blockSize   int
}

type ClientTunnel struct {
	client    *Client
	conn      net.Conn
	index     int
	totalSize int64
}

type Client struct {
	params            *ClientParams
	totalSize         int64
	startTime         time.Time
	completion        sync.WaitGroup
	pendingBlocks     *list.List
	freeBlocks        *list.List
	file              *os.File
	eof               bool
	filename          string
	fileMetadata      *FileMetadata
	fileEOF           *FileEOF
	currentBlockIndex int64
	tunnels           []*ClientTunnel
	blockChannel      chan *BlockInfo
	hasher            hash.Hash
	digest            []byte
	digeststr         string
}

func NewClientTunnel(client *Client, index int) *ClientTunnel {
	tunnel := new(ClientTunnel)
	tunnel.client = client
	tunnel.index = index
	return tunnel
}

func NewClientParams() *ClientParams {
	params := new(ClientParams)
	params.blockSize = 256 * 1024
	params.clientCount = 8
	return params
}

func NewClient() *Client {
	client := new(Client)
	client.pendingBlocks = list.New()
	client.freeBlocks = list.New()
	client.blockChannel = make(chan *BlockInfo, 10)
	client.hasher = md5.New()
	return client
}

func (client *Client) CalcSpeed() (usedTime float64, speed float64) {
	usedTime = time.Now().Sub(client.startTime).Seconds()
	speed = 0.0
	if usedTime > 0 {
		speed = float64(client.totalSize) / usedTime
	}
	return usedTime, speed
}

func (client *Client) GetBlock() *BlockInfo {
	if client.freeBlocks.Len() > 0 {
		elem := client.freeBlocks.Front()
		client.freeBlocks.Remove(elem)
		return elem.Value.(*BlockInfo)
	}
	block := new(BlockInfo)
	block.buffer = make([]byte, client.params.blockSize, client.params.blockSize)
	return block
}

func (client *Client) NewBlock(blockSize int32) *BlockInfo {
	block := new(BlockInfo)
	block.buffer = make([]byte, blockSize)
	block.size = blockSize
	return block
}

func (client *Client) ReadBlocks() {
	var blockIndex int64 = 0
	var blockCount int64 = client.fileMetadata.CalcBlockCount()
	fmt.Printf("Client.ReadBlocks %v\n", blockCount)
	for ; blockIndex < blockCount; blockIndex++ {
		block := client.ReadBlock(blockIndex)
		fmt.Printf("readBlocks pushblock index=%v\n", blockIndex)
		client.blockChannel <- block
		fmt.Printf("readBlocks pushblock ok index=%v\n", blockIndex)
	}
	client.digest = client.hasher.Sum(nil)
	client.digeststr = hex.EncodeToString(client.digest)
	client.fileEOF.digest = client.digest
	fmt.Printf("ReadBlock EOF: digest=%s\n", client.digeststr)
	close(client.blockChannel)
}

func (client *Client) ReadBlock(blockIndex int64) *BlockInfo {
	blockSize := client.fileMetadata.CalcBlockSize(blockIndex)
	fmt.Printf("ReadBlock NewBlock: %v %v\n", blockIndex, blockSize)
	/*
		if client.pendingBlocks.Len() > 0 {
			elem := client.pendingBlocks.Front()
			client.pendingBlocks.Remove(elem)
			return elem.Value.(*BlockInfo)
		}
		block := client.NewBlock()
	*/
	block := client.NewBlock(blockSize)
	//fmt.Printf("readBlock try: %v index=%v %v %v\n", client.currentBlockIndex, index, blockSize, len(block.buffer))
	size, err := io.ReadFull(client.file, block.buffer)
	if err != nil || size != int(blockSize) {
		fmt.Printf("readBlock fail: %v %v %v %v\n", blockIndex, err, len(block.buffer), size)
		return nil
	}
	fmt.Printf("ReadBlock ok: index=%v blocksize=%v size=%v\n", blockIndex, blockSize, size)
	block.size = int32(size)
	block.blockIndex = blockIndex
	client.currentBlockIndex = blockIndex
	client.hasher.Write(block.buffer[0:size])
	//fmt.Printf("Client.readBlock before inc blockIndex=%v index=%v size=%v currentBlockIndex=%v\n", block.blockIndex, index, size, client.currentBlockIndex)
	//client.currentBlockIndex += 1
	//fmt.Printf("Client.readBlock blockIndex=%v index=%v size=%v currentBlockIndex=%v\n", block.blockIndex, index, size, client.currentBlockIndex)
	return block
}

func (client *Client) OpenFile() bool {
	client.filename = filepath.Base(client.params.path)
	var err error
	client.file, err = os.Open(client.params.path)
	if err != nil {
		fmt.Printf("failed to open file: %s: %v\n", client.params.path, err)
		return false
	}
	client.fileMetadata = new(FileMetadata)
	client.fileMetadata.name = client.filename
	client.fileEOF = new(FileEOF)
	fi, err := client.file.Stat()
	client.fileMetadata.totalSize = fi.Size()
	client.fileMetadata.blockSize = int32(client.params.blockSize)
	return true
}

func (client *Client) Run(params *ClientParams) {
	fmt.Printf("run client test program\n")
	client.params = params
	client.startTime = time.Now()
	client.totalSize = 0
	if !client.OpenFile() {
		fmt.Printf("clientMain failed to open file: %s\n", client.params.path)
		return
	}
	go client.ReadBlocks()
	client.tunnels = make([]*ClientTunnel, client.params.clientCount)
	for i := 0; i < client.params.clientCount; i++ {
		client.completion.Add(1)
		tunnel := NewClientTunnel(client, i)
		client.tunnels[i] = tunnel
		go tunnel.Run()
	}
	fmt.Printf("clientMain waiting for completion %v\n", client.completion)
	client.completion.Wait()
}

func (tunnel *ClientTunnel) Run() {
	conn, err := net.Dial("tcp", tunnel.client.params.address)
	if err != nil {
		fmt.Printf("clientOne %d failed: %s\n", tunnel.index, err.Error())
		tunnel.client.completion.Done()
		return
	}
	tunnel.conn = conn
	tunnel.totalSize = 0
	WritePacket(tunnel.client.fileMetadata, conn)
	for {
		block := <-tunnel.client.blockChannel
		if block == nil {
			fmt.Printf("clientOne: end of file\n")
			break
		}
		WritePacket(block, conn)
		if err != nil {
			fmt.Printf("clientOne write failed: %v %s\n", tunnel.index, err.Error())
			break
		}
		tunnel.totalSize += int64(block.size)
		tunnel.client.totalSize += int64(block.size)
		usedTime, speed := tunnel.client.CalcSpeed()
		percent := float64(tunnel.client.totalSize) * 100.0 / float64(tunnel.client.fileMetadata.totalSize)
		fmt.Printf("clientOne stats %d blockIndex=%v total send size:%d %d percent=%.2f%% usedtime:%.2fs speed:%.2fKB/s\n", tunnel.index, tunnel.client.currentBlockIndex, tunnel.client.totalSize, tunnel.totalSize, percent, usedTime, speed/1000.0)
	}
	usedTime, speed := tunnel.client.CalcSpeed()
	fmt.Printf("!!!!!!!!!!!!!!!!! Done: clientOne %d total send size:%d usedtime:%.2fs speed:%.2fKB/s\n", tunnel.index, tunnel.client.totalSize, usedTime, speed/1000.0)
	WritePacket(tunnel.client.fileEOF, conn)
	tunnel.client.completion.Done()
}
