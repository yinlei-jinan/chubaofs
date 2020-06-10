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
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/cmd/common"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"

	"github.com/gorilla/mux"
)

// Network protocol
const (
	NetworkProtocol = "tcp"
)

type ProxyNode struct {
	sync.RWMutex

	masterProxyAddr  string
	metaProxyAddr    string
	dataProxyAddr    string
	serverIP         string
	leaderMasterAddr string
	masterAddr       []string
	mc               *master.MasterClient
	connPool         *util.ConnectPool
	closeCh          chan struct{}
	control          common.Control

	volumeInfo   *VolumeInfo
	mpHostInfo   *PartitionHostInfo
	dpHostInfo   *PartitionHostInfo
	dpHostStatus *HostStatus
}

type VolumeInfo struct {
	sync.RWMutex
	volumes []string
}

type PartitionHostInfo struct {
	sync.RWMutex
	Hosts map[uint64]*PartitionHosts
}

type PartitionHosts struct {
	LeaderAddr string
	Members    []string
}

type HostStatus struct {
	sync.RWMutex
	Status map[string]bool
}

func NewServer() *ProxyNode {
	return &ProxyNode{}
}

func (p *ProxyNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	return p.control.Start(p, cfg, doStart)
}

// Shutdown shuts down the current data node.
func (p *ProxyNode) Shutdown() {
	p.control.Shutdown(p, doShutdown)
}

// Sync keeps data node in sync.
func (p *ProxyNode) Sync() {
	p.control.Sync()
}

// Workflow of starting up a data node.
func doStart(server common.Server, cfg *config.Config) (err error) {
	p, ok := server.(*ProxyNode)
	if !ok {
		return
	}

	err = p.parseCfg(cfg)
	if err != nil {
		return
	}
	log.LogInfof("doStart: parseCfg success")

	err = p.initProxyNode()
	if err != nil {
		return
	}
	log.LogInfof("doStart: initProxyNode success")

	err = p.startMasterProxyService()
	if err != nil {
		return
	}
	log.LogInfof("doStart: startMasterProxyService success")

	err = p.startMetaProxyService()
	if err != nil {
		return
	}
	log.LogInfof("doStart: startMetaProxyService success")

	err = p.startDataProxyService()
	if err != nil {
		return
	}
	log.LogInfof("doStart: startDataProxyService success")

	return nil
}

func doShutdown(server common.Server) {
}

func (p *ProxyNode) parseCfg(cfg *config.Config) (err error) {
	masters := cfg.GetString(proto.MasterAddr)
	p.masterAddr = strings.Split(masters, ",")

	p.serverIP = cfg.GetString("serverIP")
	masterProxyPort := cfg.GetString("masterPort")
	metaProxyPort := cfg.GetString("metaPort")
	dataProxyPort := cfg.GetString("dataPort")

	p.masterProxyAddr = p.serverIP + ":" + masterProxyPort
	p.metaProxyAddr = p.serverIP + ":" + metaProxyPort
	p.dataProxyAddr = p.serverIP + ":" + dataProxyPort

	return
}

func (p *ProxyNode) initProxyNode() (err error) {
	p.mpHostInfo = new(PartitionHostInfo)
	p.dpHostInfo = new(PartitionHostInfo)
	p.dpHostStatus = new(HostStatus)
	p.volumeInfo = new(VolumeInfo)

	p.mpHostInfo.Hosts = make(map[uint64]*PartitionHosts)
	p.dpHostInfo.Hosts = make(map[uint64]*PartitionHosts)
	p.dpHostStatus.Status = make(map[string]bool)
	p.volumeInfo.volumes = append([]string{}, "ltptest")

	p.connPool = util.NewConnectPool()

	p.mc = master.NewMasterClient(p.masterAddr, false)

	err = p.updatePartitionHosts()
	if err != nil {
	}

	err = p.updateDataHostStatus()
	if err != nil {
	}

	go p.refresh()

	return
}

func (p *ProxyNode) startMasterProxyService() (err error) {
	router := mux.NewRouter().SkipClean(true)
	router.Path("/{*}").HandlerFunc(p.serverMasterRequest)
	router.Path("/{*}/{*}").HandlerFunc(p.serverMasterRequest)

	var server = &http.Server{
		Addr:    fmt.Sprintf(p.masterProxyAddr),
		Handler: router,
	}

	go func() {
		log.LogInfof("action[startMasterProxyService] start to listen to addr %v.", p.masterProxyAddr)
		err := server.ListenAndServe()
		if err != nil {
			log.LogErrorf("action[startMasterProxyService] failed to listen, err: %v", err)
			return
		}
	}()

	return
}

func (p *ProxyNode) startMetaProxyService() (err error) {
	addr := fmt.Sprintf(p.metaProxyAddr)
	listener, err := net.Listen(NetworkProtocol, addr)
	if err != nil {
		log.LogErrorf("action[startMetaProxyService] failed to listen to addr %v, err %v", p.metaProxyAddr, err)
		return
	}

	go func(ln net.Listener) {
		log.LogInfof("action[startMetaProxyService] start to listen to addr %v.", p.metaProxyAddr)
		for {
			conn, err := ln.Accept()
			if err != nil {
				break
			}
			go p.serverMetaConn(conn)
		}
	}(listener)

	return
}

func (p *ProxyNode) startDataProxyService() (err error) {
	addr := fmt.Sprintf(p.dataProxyAddr)
	listener, err := net.Listen(NetworkProtocol, addr)
	if err != nil {
		log.LogErrorf("action[startDataProxyService] failed to listen to addr %v, err %v", p.dataProxyAddr, err)
		return
	}

	go func(ln net.Listener) {
		log.LogInfof("action[startDataProxyService] start to listen to addr %v.", p.dataProxyAddr)
		for {
			conn, err := ln.Accept()
			if err != nil {
				break
			}
			go p.serverDataConn(conn)
		}
	}(listener)

	return
}

func (p *ProxyNode) refresh() {
	var err error

	t := time.NewTimer(time.Minute)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err = p.updatePartitionHosts()
			if err != nil {
			}

			err = p.updateDataHostStatus()
			if err != nil {
			}
			t.Reset(time.Minute)

		case <-p.closeCh:
			return
		}
	}
}

func (p *ProxyNode) updatePartitionHosts() (err error) {
	p.volumeInfo.RLock()
	vols := p.volumeInfo.volumes
	p.volumeInfo.RUnlock()

	err = p.updateSinglePartitionHosts(vols)
	if err != nil {
	}

	return
}

func (p *ProxyNode) updateSinglePartitionHosts(vols []string) (err error) {
	newMpHosts := make(map[uint64]*PartitionHosts)
	newDpHosts := make(map[uint64]*PartitionHosts)
	for _, vol := range vols {
		var mvs []*proto.MetaPartitionView
		mvs, err = p.mc.ClientAPI().GetMetaPartitions(vol)
		if err != nil {
			return
		}

		for _, mv := range mvs {
			if mv.LeaderAddr == "" {
				mv.LeaderAddr = mv.Members[0]
				log.LogWarnf("updateSinglePartitionHosts: MP PartitionID(%v) miss leader, set leader to first member(%v)",
					mv.PartitionID, mv.LeaderAddr)
			}
			newMpHosts[mv.PartitionID] = &PartitionHosts{
				LeaderAddr: mv.LeaderAddr,
				Members:    mv.Members,
			}
			log.LogDebugf("updateSinglePartitionHosts: updateMP PartitionID(%v) (%v)", mv.PartitionID,
				newMpHosts[mv.PartitionID])
		}

		var dvs *proto.DataPartitionsView
		dvs, err = p.mc.ClientAPI().GetDataPartitions(vol)
		if err != nil {
			return
		}

		for _, dv := range dvs.DataPartitions {
			if dv.LeaderAddr == "" {
				dv.LeaderAddr = dv.Hosts[0]
				log.LogWarnf("updateSinglePartitionHosts: DP PartitionID(%v) miss leader, set leader to first host(%v)",
					dv.PartitionID, dv.LeaderAddr)
			}
			newDpHosts[dv.PartitionID] = &PartitionHosts{
				LeaderAddr: dv.LeaderAddr,
				Members:    dv.Hosts,
			}
			log.LogDebugf("updateSinglePartitionHosts: updateDP PartitionID(%v) (%v)", dv.PartitionID,
				newDpHosts[dv.PartitionID])
		}
	}

	p.mpHostInfo.Lock()
	p.mpHostInfo.Hosts = newMpHosts
	p.mpHostInfo.Unlock()

	p.dpHostInfo.Lock()
	p.dpHostInfo.Hosts = newDpHosts
	p.dpHostInfo.Unlock()

	return
}

func (p *ProxyNode) updateDataHostStatus() (err error) {
	var cv *proto.ClusterView
	cv, err = p.mc.AdminAPI().GetCluster()
	if err != nil {
		log.LogErrorf("updateDataNodeStatus: get cluster fail: err(%v)", err)
		return
	}

	newHostsStatus := make(map[string]bool)
	for _, node := range cv.DataNodes {
		newHostsStatus[node.Addr] = node.Status
	}
	log.LogDebugf("updateDataNodeStatus: update %d hosts status", len(newHostsStatus))

	p.dpHostStatus.Lock()
	p.dpHostStatus.Status = newHostsStatus
	p.dpHostStatus.Unlock()

	return
}
