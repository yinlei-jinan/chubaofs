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
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"

	masterSDK "github.com/chubaofs/chubaofs/sdk/master"
)

const (
	MaxInitRetryLimit   = 5
	CodecUpdateInterval = time.Minute
	RetryInterval       = time.Second
)

type CodecConfig struct {
	Masters []string
}

type CodecWrapper struct {
	sync.RWMutex

	CodecNodes []proto.NodeView

	mc      *masterSDK.MasterClient
	conns   *util.ConnectPool
	closeCh chan struct{}
}

func NewCodecWrapper(mc *masterSDK.MasterClient) (cw *CodecWrapper, err error) {
	cw = new(CodecWrapper)
	cw.mc = mc
	cw.conns = util.NewConnectPool()
	cw.closeCh = make(chan struct{}, 1)

	cw.initCodecWrapper()

	go cw.refresh()

	return
}

func (cw *CodecWrapper) initCodecWrapper() (err error) {
	for i := 0; i < MaxInitRetryLimit; i++ {
		err = cw.updateCodec()
		if err == nil {
			log.LogDebugf("initCodecWrapper: updateCodec success, codec view [%v]", cw.CodecNodes)
			break
		}
		log.LogWarnf("initCodecWrapper: updateCodec failed after %d retry, err is [%v]", i, err)
		time.Sleep(RetryInterval)
	}

	log.LogErrorf("Init codecWrapper failed after max[%d] retry, last err is [%v]", MaxInitRetryLimit, err)
	return
}

func (cw *CodecWrapper) refresh() {
	var err error
	t := time.NewTimer(CodecUpdateInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err = cw.updateCodec()
			if err != nil {
				log.LogWarnf("refresh: updateCodec failed, err is [%v]", err)
				t.Reset(RetryInterval)
				continue
			}
			log.LogDebugf("refresh: updateCodec success, codec view [%v]", cw.CodecNodes)
			t.Reset(CodecUpdateInterval)
		}
	}
}

func (cw *CodecWrapper) updateCodec() (err error) {
	var view *proto.ClusterView
	view, err = cw.mc.AdminAPI().GetCluster()
	if err != nil {
		return
	}

	cw.Lock()
	cw.CodecNodes = view.CodecNodes
	cw.Unlock()

	return
}
