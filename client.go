package rclient

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
)

var (
	// ErrNotExistObject 返回空对象
	ErrNotExistObject = errors.New("client: not exist object")
	// ErrProtocolFromat protocol 格式错误
	ErrProtocolFromat = errors.New("client: wrong protocol fromat")
)

// Client 客户端结构体
type Client struct {
	host string
	port string
	conn *net.TCPConn
}

// NewClient 新创建一个客户端
func NewClient(h, p string) *Client {
	return &Client{
		host: h,
		port: p,
	}
}

// Connect 连接到redis
func (c *Client) Connect() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(c.host, c.port))
	if err != nil {
		return err
	}
	c.conn, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}
	return nil
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.conn.Close()
}

// DoRequest 发送请求
func (c *Client) DoRequest(cmd []byte, args ...[]byte) (int, error) {
	return c.conn.Write(MultiBulkMarshal(cmd, args...))
}

// GetReply 获取回复
func (c *Client) GetReply() (*Reply, error) {
	rd := bufio.NewReader(c.conn)
	b, err := rd.Peek(1)
	if err != nil {
		return nil, err
	}

	var reply *Reply
	if b[0] == byte('*') {
		multiBulk, err := MultiBulkUnMarshal(rd)
		if err != nil {
			return nil, err
		}
		reply = &Reply{
			multiBulk: multiBulk,
			isMulti:   true,
		}
	} else {
		bulk, err := BulkUnMarshal(rd)
		if err != nil {
			return nil, err
		}
		reply = &Reply{
			bulk:    bulk,
			isMulti: false,
		}
	}

	return reply, nil
}

// Reply 解析redis回复结构
type Reply struct {
	isMulti   bool
	bulk      []byte
	multiBulk [][]byte
}

// Format 格式化redis返回值
func (r *Reply) Format() []string {

	// 单条回复
	if !r.isMulti {
		if r.bulk == nil {
			return []string{fmt.Sprint("(nil)")}
		}
		return []string{fmt.Sprint(string(r.bulk))}
	}

	// 多条批量回复
	if r.multiBulk == nil {
		return []string{fmt.Sprint("(nil)")}
	}
	if len(r.multiBulk) == 0 {
		return []string{fmt.Sprint("(empty list or set)")}
	}
	out := make([]string, len(r.multiBulk))
	for i := 0; i < len(r.multiBulk); i++ {
		if r.multiBulk[i] == nil {
			out[i] = fmt.Sprintf("%d) (nil)", i)
		} else {
			out[i] = fmt.Sprintf("%d) \"%s\"", i, r.multiBulk[i])
		}
	}
	return out
}

// BulkUnMarshal 单条回复
func BulkUnMarshal(rd *bufio.Reader) ([]byte, error) {
	b, err := rd.ReadByte()
	if err != nil {
		return []byte{}, err
	}

	var result []byte
	switch b {
	case byte('+'), byte('-'), byte(':'):
		r, _, err := rd.ReadLine()
		if err != nil {
			return []byte{}, err
		}
		result = r
	case byte('$'):
		r, _, err := rd.ReadLine()
		if err != nil {
			return []byte{}, err
		}

		l, err := strconv.Atoi(string(r))
		if err != nil {
			return []byte{}, err
		}

		// 空白元素回复, 客户端应该返回空对象，而不是空字符串
		if l == -1 {
			return nil, nil
		}

		// 必须读走最后的\r\n 否则在解析*的时候会读到
		p := make([]byte, l+2)
		rd.Read(p)
		result = p[0 : len(p)-2]
	}

	return result, nil
}

// MultiBulkUnMarshal 多条批量回复
func MultiBulkUnMarshal(rd *bufio.Reader) ([][]byte, error) {
	b, err := rd.ReadByte()
	if err != nil {
		return [][]byte{}, err
	}

	if b != '*' {
		return [][]byte{}, ErrProtocolFromat
	}

	// 解析回复数量
	bNum, _, err := rd.ReadLine()
	if err != nil {
		return [][]byte{}, err
	}
	n, err := strconv.Atoi(string(bNum))
	if err != nil {
		return [][]byte{}, err
	}

	// 多条批量回复也可以是空白的（empty)
	if n == 0 {
		return [][]byte{}, nil
	}

	// 无内容的多条批量回复（null multi bulk reply）也是存在的,
	// 客户端库应该返回一个 null 对象, 而不是一个空数组。
	if n == -1 {
		return nil, nil
	}

	// 循环解析单条批量回复
	result := make([][]byte, n)
	for i := 0; i < n; i++ {
		result[i], err = BulkUnMarshal(rd)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

// MultiBulkMarshal 多条请求协议
func MultiBulkMarshal(cmd []byte, args ...[]byte) []byte {
	buffer := new(bytes.Buffer)
	buffer.WriteByte('*')
	buffer.WriteString(strconv.Itoa(len(args) + 1))
	buffer.Write([]byte{'\r', '\n'})

	// 命令本身也作为协议的其中一个参数来发送
	buffer.WriteByte('$')
	buffer.WriteString(strconv.Itoa(len(cmd)))
	buffer.Write([]byte{'\r', '\n'})
	buffer.Write(cmd)
	buffer.Write([]byte{'\r', '\n'})

	// 命令所有参数
	for _, v := range args {
		buffer.WriteByte('$')
		buffer.WriteString(strconv.Itoa(len(v)))
		buffer.Write([]byte{'\r', '\n'})
		buffer.Write(v)
		buffer.Write([]byte{'\r', '\n'})
	}

	return buffer.Bytes()
}
