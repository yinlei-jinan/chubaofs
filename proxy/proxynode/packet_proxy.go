// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package proxynode

import (
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
)

// packetProxy used to redirect packet form srcConn to dest host, and redirect host's response to srcConn
func (p *ProxyNode) packetProxy(packet *repl.Packet, srcConn *net.TCPConn, host string) (err error) {
	destConn, err := p.connPool.GetConnect(host)
	if err != nil {
		return errors.New(fmt.Sprintf("GetConnect failed, host [%v], err [%v]", host, err))
	}
	defer p.connPool.PutConnect(destConn, true)

	err = packet.WriteHeaderToConn(destConn, proto.WriteDeadlineTime)
	if err != nil {
		return errors.New(fmt.Sprintf("WriteHeader failed, to destConn [%v], err [%v]", destConn, err))
	}

	// For OpStreamFollowerRead/OpStreamRead, packet.Size does not represent data length. It represent the data length
	// it wants from the reply
	if (packet.Size > 0) && (packet.Opcode != proto.OpStreamFollowerRead) && (packet.Opcode != proto.OpStreamRead) {
		_, err = io.CopyN(destConn, srcConn, int64(packet.Size))
		if err != nil {
			return errors.New(fmt.Sprintf("io.Copy failed, to destConn [%v], from srcConn [%v], size [%v], err [%v]",
				destConn, srcConn, packet.Size, err))
		}
	}

	resp := repl.NewPacket()
	err = resp.ReadHeaderFromConn(destConn, proto.ReadDeadlineTime)
	if err != nil {
		return errors.New(fmt.Sprintf("ReadHeader failed, from destConn [%v], err [%v]", destConn, err))
	}

	err = resp.WriteHeaderToConn(srcConn, proto.WriteDeadlineTime)
	if err != nil {
		return errors.New(fmt.Sprintf("WriteHeader failed, to srcConn [%v], err [%v]", srcConn, err))
	}

	if resp.Size > 0 {
		_, err = io.CopyN(srcConn, destConn, int64(resp.Size))
		if err != nil {
			return errors.New(fmt.Sprintf("io.Copy falied, from destConn [%v], to srcConn [%v], size [%v], err [%v]",
				destConn, srcConn, packet.Size, err))
		}
	}

	return
}
