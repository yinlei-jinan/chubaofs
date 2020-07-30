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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/chubaofs/chubaofs/proto"
)

func (cw *CodecWrapper) issueMigrationTask(inodes []uint64, metaPartitionId uint64, volName string,
	codecNodeAddr string) (err error) {
	reqData := &proto.IssueMigrationTaskRequest{
		VolName:     volName,
		PartitionId: metaPartitionId,
		Inodes:      inodes,
	}

	p := proto.NewPacket()
	p.Opcode = proto.OpIssueMigrationTask
	p.PartitionID = metaPartitionId
	p.Data, err = json.Marshal(reqData)
	if err != nil {
		return
	}
	p.Size = uint32(len(p.Data))
	p.ReqID = proto.GenerateRequestID()

	err = cw.sendToCodec(codecNodeAddr, p)
	if err != nil {
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.New(fmt.Sprintf("[IssueMigrationTaskRequest] %s response: %s", p.GetUniqueLogId(),
			p.GetResultMsg()))
	}

	return
}
