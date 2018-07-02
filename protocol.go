package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	FILE_METADATA_PACKET = 1
	BLOCK_PACKET         = 2
	FILE_EOF_PACKET      = 3
)

type Packet interface {
	GetPacketType() byte
	Write(writer io.Writer)
	Read(reader io.Reader) error
	CalcSize() int
}

type PacketHeader struct {
	packetType byte
}

type FileMetadata struct {
	name      string
	totalSize int64
	blockSize int32
}

type FileEOF struct {
	digest []byte
}

func (metadata *FileMetadata) GetPacketType() byte {
	return FILE_METADATA_PACKET
}

func (metadata *FileMetadata) CalcBlockSize(blockIndex int64) int32 {
	blockCount := metadata.CalcBlockCount()
	if blockIndex < 0 || blockIndex >= blockCount || blockCount <= 0 {
		return -1
	}
	if blockIndex < blockCount-1 {
		return metadata.blockSize
	}
	return int32(metadata.totalSize - int64(metadata.blockSize)*(blockCount-1))
}

func (metadata *FileMetadata) CalcBlockCount() int64 {
	blockSize := int64(metadata.blockSize)
	return (metadata.totalSize + blockSize - 1) / blockSize
}

func (metadata *FileMetadata) Write(writer io.Writer) {
	fmt.Printf("FileMetadata.Write %v %v\n", metadata.name, uint16(len(metadata.name)))
	err := binary.Write(writer, binary.BigEndian, uint16(len(metadata.name)))
	size, err := writer.Write([]byte(metadata.name))
	err = binary.Write(writer, binary.BigEndian, metadata.totalSize)
	err = binary.Write(writer, binary.BigEndian, metadata.blockSize)
	fmt.Printf("FileMetadata.Write %v %v\n", err, size)
}

func (metadata *FileMetadata) Read(reader io.Reader) error {
	var namelen uint16
	err := binary.Read(reader, binary.BigEndian, &namelen)
	fmt.Printf("Read name: %d %v \n", namelen, err)
	namebuf := make([]byte, namelen)
	io.ReadFull(reader, namebuf)
	fmt.Printf("Read name: %d %d\n", len(namebuf), namelen)
	if len(namebuf) != int(namelen) {
		fmt.Printf("Read name failed %d %d\n", len(namebuf), namelen)
		return nil
	}
	metadata.name = string(namebuf)
	binary.Read(reader, binary.BigEndian, &metadata.totalSize)
	binary.Read(reader, binary.BigEndian, &metadata.blockSize)
	return nil
}

func (metadata *FileMetadata) CalcSize() int {
	return 2 + len(metadata.name) + 8 + 4
}

func WritePacket(packet Packet, writer io.Writer) {
	packetType := []byte{packet.GetPacketType()}
	binary.Write(writer, binary.BigEndian, packetType)
	packet.Write(writer)
}

type BlockInfo struct {
	blockIndex int64
	size       int32
	buffer     []byte
	//offset     int
}

func (block *BlockInfo) GetPacketType() byte {
	return BLOCK_PACKET
}

func (block *BlockInfo) GetData() []byte {
	return block.buffer[0:block.size]
}

func (block *BlockInfo) Write(writer io.Writer) {
	err := binary.Write(writer, binary.BigEndian, block.blockIndex)
	err = binary.Write(writer, binary.BigEndian, block.size)
	size, err := writer.Write(block.GetData())
	if err != nil {
		fmt.Printf("BlockInfo.Write %v %v\n", err, size)
	}
}

func (block *BlockInfo) Read(reader io.Reader) error {
	err := binary.Read(reader, binary.BigEndian, &block.blockIndex)
	err = binary.Read(reader, binary.BigEndian, &block.size)
	block.buffer = make([]byte, block.size)
	size, err := io.ReadFull(reader, block.buffer)
	if err != nil {
		fmt.Printf("BlockInfo.Read fail: %v %v\n", err, size)
		return err
	}
	return nil
}

func (block *BlockInfo) CalcSize() int {
	return 8 + 4 + len(block.buffer)
}

func (fe *FileEOF) GetPacketType() byte {
	return FILE_EOF_PACKET
}

func (fe *FileEOF) Write(writer io.Writer) {
	fmt.Printf("FileEOF.Write %v\n", len(fe.digest))
	err := binary.Write(writer, binary.BigEndian, uint16(len(fe.digest)))
	size, err := writer.Write(fe.digest)
	fmt.Printf("FileEOF.Write %v %v\n", err, size)
}

func (fe *FileEOF) Read(reader io.Reader) error {
	var digestlen uint16
	err := binary.Read(reader, binary.BigEndian, &digestlen)
	fmt.Printf("FileEOF.Read digestlen: %d %v \n", digestlen, err)
	if err != nil {
		return err
	}
	digestbuf := make([]byte, digestlen)
	size, err := io.ReadFull(reader, digestbuf)
	fmt.Printf("FileEOF.Read name: %d %d\n", len(digestbuf), digestlen)
	if err != nil || len(digestbuf) != int(digestlen) {
		fmt.Printf("FileEOF.Read digest failed size=%v %d %d\n", size, len(digestbuf), digestlen)
		return err
	}
	fe.digest = digestbuf
	return nil
}

func (fe *FileEOF) CalcSize() int {
	return 2 + len(fe.digest)
}
