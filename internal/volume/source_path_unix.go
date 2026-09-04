//go:build !windows

package volume

func sourcePathComponents(tenant, id string) (string, string) {
	return tenant, id
}
