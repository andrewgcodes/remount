package node

import (
	"sort"

	"remount.dev/remount/internal/proto"
)

func (n *Node) controllerState() (*proto.ControllerNodeState, error) {
	result := &proto.ControllerNodeState{Node: n.id, Epoch: n.currentControllerEpoch()}
	n.mu.Lock()
	for _, workspace := range n.workspaces {
		var copied proto.Workspace
		if err := proto.Unmarshal(proto.MustMarshal(&workspace.Workspace), &copied); err != nil {
			n.mu.Unlock()
			return nil, err
		}
		result.Workspaces = append(result.Workspaces, proto.ControllerWorkspace{Workspace: copied})
	}
	n.mu.Unlock()
	for index := range result.Workspaces {
		for _, session := range n.sessions.List(result.Workspaces[index].Workspace.ID) {
			result.Workspaces[index].Sessions = append(result.Workspaces[index].Sessions, session.ID)
		}
		sort.Strings(result.Workspaces[index].Sessions)
	}
	n.releaseMu.Lock()
	for _, release := range n.releases {
		result.Releases = append(result.Releases, proto.ControllerReleaseState{
			Request: release.Request, Response: release.Response, OperationID: release.OperationID, State: release.State,
		})
	}
	n.releaseMu.Unlock()
	sort.Slice(result.Workspaces, func(i, j int) bool { return result.Workspaces[i].Workspace.ID < result.Workspaces[j].Workspace.ID })
	sort.Slice(result.Releases, func(i, j int) bool { return result.Releases[i].Request.WS < result.Releases[j].Request.WS })
	return result, nil
}
