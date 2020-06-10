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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

func redirectRequestToMaster(r *http.Request, master string) (respData []byte, err error) {
	outUrl := fmt.Sprintf("http://%s%s", master, r.URL)
	outReq, err := http.NewRequest(r.Method, outUrl, r.Body)
	if err != nil {
		return
	}
	outReq.Header = r.Header

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	outRespData, err := client.Do(outReq)
	if err != nil {
		return
	}

	stateCode := outRespData.StatusCode
	if (stateCode != http.StatusForbidden) && (stateCode != http.StatusOK) {
		err = errors.New(fmt.Sprintf("unknown status: uri(%v) status(%v) body(%v).", outUrl, stateCode,
			string(respData)))
		return
	}

	respData, err = ioutil.ReadAll(outRespData.Body)
	if err != nil {
		return
	}

	return
}

func (p *ProxyNode) redirectRequest(r *http.Request) (respData []byte, errs error) {
	p.RLock()
	leaderMaster := p.leaderMasterAddr
	p.RUnlock()

	respData, err := redirectRequestToMaster(r, leaderMaster)
	if err == nil {
		return
	}

	errs = errors.New(fmt.Sprintf("[%v]", err))

	for i := 0; i < len(p.masterAddr); i++ {
		p.RLock()
		nextMaster := p.masterAddr[i]
		p.RUnlock()
		if leaderMaster == nextMaster {
			continue
		}

		respData, err = redirectRequestToMaster(r, nextMaster)
		if err == nil {
			p.Lock()
			p.leaderMasterAddr = nextMaster
			p.Unlock()
			errs = nil
			return
		}
		errs = errors.New(fmt.Sprintf("%v-[%v]", errs, err))
	}

	return
}

func (p *ProxyNode) rebuildView(w http.ResponseWriter, r *http.Request, srcData []byte) (data []byte, err error) {
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	err = json.Unmarshal(srcData, body)
	if err != nil {
		return
	}

	var viewData json.RawMessage
	switch r.URL.Path {
	case proto.ClientVol:
		viewData, err = p.rebuildClientVol(body.Data)
	case proto.ClientDataPartitions:
		viewData, err = p.rebuildClientDataPartitions(body.Data)
	case proto.AdminGetCluster:
		viewData, err = p.rebuildAdminGetCluster(body.Data)
	//case proto.AdminGetVol:
	//case proto.ClientVolStat:
	//case proto.UserGetAKInfo:
	//case proto.AdminGetIP:
	default:
		log.LogWarnf("recieve unexpected master request PATH [%v], URL [%v]", r.URL.Path, r.URL)
	}

	body.Data = viewData
	data, err = json.Marshal(body)
	return
}

func (p *ProxyNode) serverMasterRequest(w http.ResponseWriter, r *http.Request) {
	data, err := p.redirectRequest(r)
	if err != nil {
		sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
		return
	}

	if needRebuild(r.URL.Path) {
		newData, err := p.rebuildView(w, r, data)
		if err != nil {
			sendErrReply(w, r, newErrHTTPReply(proto.ErrVolNotExists))
			return
		}
		send(w, r, newData)
		log.LogDebugf("finish rebuild request URL [%v]", r.URL)
		return
	}

	send(w, r, data)

	return
}

func send(w http.ResponseWriter, r *http.Request, reply []byte) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%v]. len[%d] URL[%v] remoteAddr[%v] err:[%v]", reply, len(reply), r.URL,
			r.RemoteAddr, err)
		return
	}
	log.LogDebugf("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	return
}

func newErrHTTPReply(err error) *proto.HTTPReply {
	code, ok := proto.Err2CodeMap[err]
	if ok {
		return &proto.HTTPReply{Code: code, Msg: err.Error()}
	}
	return &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()}
}

func sendErrReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	log.LogInfof("URL[%v],remoteAddr[%v],response err[%v]", r.URL, r.RemoteAddr, httpReply)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v] remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr,
			err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%v]. len[%d] URL[%v] remoteAddr[%v] err:[%v]", reply, len(reply), r.URL,
			r.RemoteAddr, err)
	}
	return
}

func needRebuild(url string) bool {
	if (url == proto.ClientVol) ||
		(url == proto.ClientDataPartitions) ||
		(url == proto.AdminGetCluster) {
		return true
	}
	return false
}

func (p *ProxyNode) rebuildClientVol(inData json.RawMessage) (outData json.RawMessage, err error) {
	view := &proto.VolView{}
	err = json.Unmarshal(inData, view)
	if err != nil {
		return
	}
	for i := 0; i < len(view.MetaPartitions); i++ {
		view.MetaPartitions[i].LeaderAddr = p.metaProxyAddr
		view.MetaPartitions[i].Members = []string{
			p.metaProxyAddr,
			p.metaProxyAddr,
			p.metaProxyAddr,
		}
	}
	for i := 0; i < len(view.DataPartitions); i++ {
		view.DataPartitions[i].LeaderAddr = p.dataProxyAddr
		view.DataPartitions[i].Hosts = []string{
			p.dataProxyAddr,
			p.dataProxyAddr,
			p.dataProxyAddr,
		}
	}

	outData, err = json.Marshal(view)
	return
}

func (p *ProxyNode) rebuildClientDataPartitions(inData json.RawMessage) (outData json.RawMessage, err error) {
	view := &proto.DataPartitionsView{}
	err = json.Unmarshal(inData, view)
	if err != nil {
		return
	}

	for i := 0; i < len(view.DataPartitions); i++ {
		view.DataPartitions[i].LeaderAddr = p.dataProxyAddr
		view.DataPartitions[i].Hosts = []string{
			p.dataProxyAddr,
			p.dataProxyAddr,
			p.dataProxyAddr,
		}
	}

	outData, err = json.Marshal(view)
	return
}

func (p *ProxyNode) rebuildAdminGetCluster(inData json.RawMessage) (outData json.RawMessage, err error) {
	view := &proto.ClusterView{}
	err = json.Unmarshal(inData, view)
	if err != nil {
		return
	}

	view.LeaderAddr = p.masterProxyAddr
	for i := 0; i < len(view.MetaNodes); i++ {
		view.MetaNodes[i].Addr = p.metaProxyAddr
	}
	for i := 0; i < len(view.DataNodes); i++ {
		view.DataNodes[i].Addr = p.dataProxyAddr
	}

	outData, err = json.Marshal(view)
	return
}
