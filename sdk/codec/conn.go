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

package codec

import (
	"github.com/chubaofs/chubaofs/proto"
)

func (cw *CodecWrapper) sendToCodec(codecNodeAddr string, req *proto.Packet) (err error) {
	conn, err := cw.conns.GetConnect(codecNodeAddr)
	defer func() {
		if err != nil {
			cw.conns.PutConnect(conn, true)
		} else {
			cw.conns.PutConnect(conn, false)
		}
	}()

	if err != nil {
		return
	}

	err = req.WriteToConn(conn)
	if err != nil {
		return
	}

	err = req.ReadFromConn(conn, proto.ReadDeadlineTime)

	return
}
