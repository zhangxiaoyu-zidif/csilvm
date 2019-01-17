package csilvm

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"golang.org/x/sync/singleflight"
)

var _ = csi.IdentityServer(&identityArbiter{}) // sanity check

type identityArbiter struct {
	csi.IdentityServer
	g *singleflight.Group
}

func IdentityArbiter(server csi.IdentityServer) csi.IdentityServer {
	return &identityArbiter{
		IdentityServer: server,
		g:              new(singleflight.Group),
	}
}

// Probe can take some time to execute: merge all in-flight Probe requests.
func (v *identityArbiter) Probe(
	ctx context.Context,
	request *csi.ProbeRequest) (*csi.ProbeResponse, error) {

	const key = "k" // all probe requests are identical, merge all concurrent req's
	worker := func() (interface{}, error) { return v.IdentityServer.Probe(ctx, request) }
	ch := v.g.DoChan(key, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.ProbeResponse), nil
	}
}

var _ = csi.ControllerServer(&controllerArbiter{}) // sanity check

type controllerArbiter struct {
	csi.ControllerServer
	g          *singleflight.Group
	removeLock sync.RWMutex // removeLock is write-locked for remove ops and read-locked for non-remove ops that could conflict w/ remove.
}

// ControllerArbiter decorates a ControllerServer and also returns a Locker that can be
// used to coordinate operations that should not overlap w/ specific controller ops.
func ControllerArbiter(server csi.ControllerServer) (csi.ControllerServer, sync.Locker) {
	arb := &controllerArbiter{
		ControllerServer: server,
		g:                new(singleflight.Group),
	}
	return arb, arb.removeLock.RLocker()
}

// CreateVolume merges idempotent requests; waits for any pending RemoveVolume ops to complete.
func (v *controllerArbiter) CreateVolume(
	ctx context.Context,
	request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.CreateVolume(ctx, request)
	}

	// TODO(jdef) compute a worker key based on the whole request, not just the volume name.
	const ns = "new/"
	ch := v.g.DoChan(ns+request.Name, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.CreateVolumeResponse), nil
	}
}

func (v *controllerArbiter) DeleteVolume(
	ctx context.Context,
	request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.Lock()
		defer v.removeLock.Unlock()
		return v.ControllerServer.DeleteVolume(ctx, request)
	}

	const ns = "del/"
	ch := v.g.DoChan(ns+request.VolumeId, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.DeleteVolumeResponse), nil
	}
}
func (v *controllerArbiter) ControllerPublishVolume(
	ctx context.Context,
	request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.ControllerPublishVolume(ctx, request)
	}
	const ns = "pub/"
	ch := v.g.DoChan(ns+request.VolumeId, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.ControllerPublishVolumeResponse), nil
	}
}

func (v *controllerArbiter) ControllerUnpublishVolume(
	ctx context.Context,
	request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.ControllerUnpublishVolume(ctx, request)
	}
	const ns = "unpub/"
	ch := v.g.DoChan(ns+request.VolumeId, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.ControllerUnpublishVolumeResponse), nil
	}
}

func (v *controllerArbiter) ValidateVolumeCapabilities(
	ctx context.Context,
	request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.ValidateVolumeCapabilities(ctx, request)
	}
	const ns = "validate/"
	ch := v.g.DoChan(ns+request.VolumeId, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.ValidateVolumeCapabilitiesResponse), nil
	}
}

func (v *controllerArbiter) ControllerGetCapabilities(
	ctx context.Context,
	request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {

	worker := func() (interface{}, error) {
		return v.ControllerServer.ControllerGetCapabilities(ctx, request)
	}
	const ns = "caps"
	ch := v.g.DoChan(ns, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.ControllerGetCapabilitiesResponse), nil
	}
}

func (v *controllerArbiter) ListVolumes(
	ctx context.Context,
	request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.ListVolumes(ctx, request)
	}
	const ns = "list/"
	ch := v.g.DoChan(ns+request.StartingToken, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.ListVolumesResponse), nil
	}
}

func (v *controllerArbiter) GetCapacity(
	ctx context.Context,
	request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {

	type Key struct { // json encoding has predictable key order
		MountFS    []string          `json:"fs,omitempty"`
		Parameters map[string]string `json:"p,omitempty"`
	}
	key := Key{
		MountFS: func() (fs []string) {
			for _, vc := range request.GetVolumeCapabilities() {
				if m := vc.GetMount(); m != nil {
					if m.FsType != "" {
						fs = append(fs, m.FsType)
					}
				}
				if b := vc.GetBlock(); b != nil {
					// we need a placeholder...
					fs = append(fs, "**block")
				}
			}
			sort.Strings(fs)
			return
		}(),
		Parameters: request.Parameters,
	}
	buf, err := json.Marshal(&key)
	if err != nil {
		return nil, err
	}

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.GetCapacity(ctx, request)
	}
	const ns = "cap/"
	ch := v.g.DoChan(ns+string(buf), worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.GetCapacityResponse), nil
	}
}

var _ = csi.NodeServer(&nodeArbiter{}) // sanity check

type nodeArbiter struct {
	csi.NodeServer
	g    *singleflight.Group
	lock sync.Locker
}

func NodeArbiter(server csi.NodeServer, lock sync.Locker) csi.NodeServer {
	return &nodeArbiter{
		NodeServer: server,
		g:          new(singleflight.Group),
		lock:       lock,
	}
}
func (v *nodeArbiter) NodePublishVolume(
	ctx context.Context,
	request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.lock.Lock()
		defer v.lock.Unlock()
		return v.NodeServer.NodePublishVolume(ctx, request)
	}
	const ns = "pub/"

	// TODO(jdef) use more than just volumeID,staging/target paths
	key := ns + request.VolumeId + "*" + request.StagingTargetPath + "*" + request.TargetPath
	ch := v.g.DoChan(key, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.NodePublishVolumeResponse), nil
	}
}

func (v *nodeArbiter) NodeUnpublishVolume(
	ctx context.Context,
	request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.lock.Lock()
		defer v.lock.Unlock()
		return v.NodeServer.NodeUnpublishVolume(ctx, request)
	}
	const ns = "unpub/"
	key := ns + request.VolumeId + "*" + request.TargetPath
	ch := v.g.DoChan(key, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.NodeUnpublishVolumeResponse), nil
	}
}

func (v *nodeArbiter) NodeGetCapabilities(
	ctx context.Context,
	request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {

	worker := func() (interface{}, error) {
		v.lock.Lock()
		defer v.lock.Unlock()
		return v.NodeServer.NodeGetCapabilities(ctx, request)
	}
	const ns = "caps"
	ch := v.g.DoChan(ns, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.NodeGetCapabilitiesResponse), nil
	}
}

func (v *nodeArbiter) NodeGetId(
	ctx context.Context,
	request *csi.NodeGetIdRequest) (*csi.NodeGetIdResponse, error) {

	worker := func() (interface{}, error) {
		v.lock.Lock()
		defer v.lock.Unlock()
		return v.NodeServer.NodeGetId(ctx, request)
	}
	const ns = "id"
	ch := v.g.DoChan(ns, worker)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*csi.NodeGetIdResponse), nil
	}
}

func (v *nodeArbiter) NodeStageVolume(
	ctx context.Context,
	request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {

	// not yet supported by the plugin, why coalesce?
	return v.NodeServer.NodeStageVolume(ctx, request)
}

func (v *nodeArbiter) NodeUnstageVolume(
	ctx context.Context,
	request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {

	// not yet supported by the plugin, why coalesce?
	return v.NodeServer.NodeUnstageVolume(ctx, request)
}
