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

func (p *ProxyNode) serverMetaConn(conn net.Conn) (err error) {
	c, _ := conn.(*net.TCPConn)
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
				log.LogErrorf("serverMetaConn: err [%v]", err.Error())
			}
			return
		}

		err = p.serverMetaRequest(request, c)
		if err != nil {
			log.LogErrorf("serverMetaConn: proxy packet [%v] failed, err [%v]", request, err)
		}
	}
}

func (p *ProxyNode) serverMetaRequest(packet *repl.Packet, srcConn *net.TCPConn) (err error) {
	startT := time.Now()
	log.LogDebugf("serverMetaRequest: recieve packet [%v]", packet)
	err, host := p.validMetaRequest(packet)
	if err != nil {
		return
	}

	err = p.packetProxy(packet, srcConn, host)
	if err != nil {
		return
	}
	log.LogDebugf("serverMetaRequest: finish packet [%v], cost [%v]", packet, time.Since(startT))

	return
}

// validMetaRequest will check MetaRequest and find destination host
func (p *ProxyNode) validMetaRequest(packet *repl.Packet) (err error, host string) {
	_, ok := p.mpHostInfo.Hosts[packet.PartitionID]
	if !ok {
		err = errors.New(fmt.Sprintf("validMetaRequest: can not find metanode for packet [%v]", packet))
		return
	}

	log.LogDebugf("validMetaRequest: find meta partition host info [%v]", p.mpHostInfo.Hosts[packet.PartitionID])
	host = p.mpHostInfo.Hosts[packet.PartitionID].LeaderAddr

	return
}
