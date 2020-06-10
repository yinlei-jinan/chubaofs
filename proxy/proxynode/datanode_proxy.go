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
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/repl"
	"github.com/chubaofs/chubaofs/util/log"
)

func (p *ProxyNode) serverDataConn(srcConn net.Conn) (err error) {
	c, _ := srcConn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)

	for {
		select {
		default:
		}
		request := repl.NewPacket()
		err = request.ReadHeaderFromConn(c, proto.NoReadDeadlineTime)
		if err != nil {
			if err != io.EOF {
				log.LogErrorf("serverDataConn: err [%v] ", err.Error())
			}
			return
		}

		err = p.serverDataRequest(request, c)
		if err != nil {
			log.LogErrorf("serverDataConn: proxy packet [%v] failed, err [%v]", request, err)
		}
	}
}

func (p *ProxyNode) serverDataRequest(packet *repl.Packet, srcConn *net.TCPConn) (err error) {
	startT := time.Now()
	log.LogDebugf("serverDataRequest: recieve packet [%v]", packet)
	err, host := p.validDataRequest(packet)
	if err != nil {
		return
	}

	err = p.packetProxy(packet, srcConn, host)
	if err != nil {
		return
	}
	log.LogDebugf("serverDataRequest: finish packet [%v], cost [%v]", packet, time.Since(startT))

	return
}

func (p *ProxyNode) validDataRequest(packet *repl.Packet) (err error, host string) {
	hosts, ok := p.dpHostInfo.Hosts[packet.PartitionID]
	if !ok {
		err = errors.New(fmt.Sprintf("validDataRequest: can not find datanode for packet [%v]", packet))
		return
	}

	host = hosts.Members[0]
	if packet.ArgLen > 0 {
		newArg := hosts.Members[1] + proto.AddrSplit + hosts.Members[2] + proto.AddrSplit
		packet.Arg = []byte(newArg)
		packet.ArgLen = uint32(len(packet.Arg))
		log.LogDebugf("validDataRequest: valid packet.Arg [%v]", string(packet.Arg))
	}

	return
}
