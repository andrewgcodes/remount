package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"remount.dev/remount/internal/control"
	"remount.dev/remount/internal/provision"
	"remount.dev/remount/internal/provision/e2b"
	"remount.dev/remount/internal/provision/fly"
	"remount.dev/remount/internal/provision/ix"
	"remount.dev/remount/internal/provision/modal"
	"remount.dev/remount/internal/provision/ssh"
)

type provisionerFile struct {
	Bootstrap control.PoolBootstrap `json:"bootstrap"`
	Drivers   []provisionerEntry    `json:"drivers"`
}

type provisionerEntry struct {
	Vendor         string `json:"vendor"`
	Endpoint       string `json:"endpoint,omitempty"`
	TokenEnv       string `json:"token_env,omitempty"`
	App            string `json:"app,omitempty"`
	Image          string `json:"image,omitempty"`
	Template       string `json:"template,omitempty"`
	Helper         string `json:"helper,omitempty"`
	Environment    string `json:"environment,omitempty"`
	TokenIDEnv     string `json:"token_id_env,omitempty"`
	TokenSecretEnv string `json:"token_secret_env,omitempty"`
	Tenant         string `json:"tenant,omitempty"`
	Pool           string `json:"pool,omitempty"`
	Host           string `json:"host,omitempty"`
	Port           int    `json:"port,omitempty"`
	User           string `json:"user,omitempty"`
	IdentityFile   string `json:"identity_file,omitempty"`
	KnownHostsFile string `json:"known_hosts_file,omitempty"`
	RemoteHelper   string `json:"remote_helper,omitempty"`
	Region         string `json:"region,omitempty"`
	Size           string `json:"size,omitempty"`
	// E2B: where sandboxes are reachable and where the bootstrap file lands.
	SandboxDomain string `json:"sandbox_domain,omitempty"`
	BootstrapPath string `json:"bootstrap_path,omitempty"`
	BootstrapUser string `json:"bootstrap_user,omitempty"`
}

func loadProvisioners(path string) ([]provision.Driver, control.PoolBootstrap, error) {
	if path == "" {
		return nil, control.PoolBootstrap{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, control.PoolBootstrap{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config provisionerFile
	if err := decoder.Decode(&config); err != nil {
		return nil, control.PoolBootstrap{}, fmt.Errorf("decode provisioners: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, control.PoolBootstrap{}, errors.New("decode provisioners: multiple JSON values")
		}
		return nil, control.PoolBootstrap{}, fmt.Errorf("decode provisioners: %w", err)
	}
	drivers := make([]provision.Driver, 0, len(config.Drivers))
	seen := map[string]bool{}
	for _, entry := range config.Drivers {
		vendor := strings.ToLower(strings.TrimSpace(entry.Vendor))
		if vendor == "" || seen[vendor] {
			return nil, control.PoolBootstrap{}, fmt.Errorf("provisioner vendor %q is empty or duplicated", entry.Vendor)
		}
		seen[vendor] = true
		driver, err := buildProvisioner(vendor, entry)
		if err != nil {
			return nil, control.PoolBootstrap{}, fmt.Errorf("configure %s provisioner: %w", vendor, err)
		}
		drivers = append(drivers, driver)
	}
	if len(drivers) == 0 {
		return nil, control.PoolBootstrap{}, errors.New("provisioners file contains no drivers")
	}
	return drivers, config.Bootstrap, nil
}

func buildProvisioner(vendor string, entry provisionerEntry) (provision.Driver, error) {
	switch vendor {
	case "fly":
		token, err := requiredProvisionEnv(entry.TokenEnv)
		if err != nil {
			return nil, err
		}
		return fly.New(fly.Config{Endpoint: entry.Endpoint, Token: token, App: entry.App, Image: entry.Image})
	case "e2b":
		key, err := requiredProvisionEnv(entry.TokenEnv)
		if err != nil {
			return nil, err
		}
		return e2b.New(e2b.Config{Endpoint: entry.Endpoint, APIKey: key, Template: entry.Template,
			SandboxDomain: entry.SandboxDomain, BootstrapPath: entry.BootstrapPath, BootstrapUser: entry.BootstrapUser})
	case "modal":
		id, err := requiredProvisionEnv(entry.TokenIDEnv)
		if err != nil {
			return nil, err
		}
		secret, err := requiredProvisionEnv(entry.TokenSecretEnv)
		if err != nil {
			return nil, err
		}
		return modal.New(modal.Config{Helper: entry.Helper, App: entry.App, Image: entry.Image,
			Environment: entry.Environment, TokenID: id, TokenSecret: secret})
	case "ix":
		key, err := requiredProvisionEnv(entry.TokenEnv)
		if err != nil {
			return nil, err
		}
		return ix.New(ix.Config{Helper: entry.Helper, APIKey: key,
			Region: entry.Region, Tenant: entry.Tenant, Pool: entry.Pool})
	case "ssh":
		return ssh.New(ssh.Config{Host: entry.Host, Port: entry.Port, User: entry.User, IdentityFile: entry.IdentityFile,
			KnownHostsFile: entry.KnownHostsFile, RemoteHelper: entry.RemoteHelper, Tenant: entry.Tenant,
			Pool: entry.Pool, Region: entry.Region, Size: entry.Size})
	default:
		return nil, fmt.Errorf("unsupported vendor %q", vendor)
	}
}

func requiredProvisionEnv(name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", errors.New("credential environment variable name is required")
	}
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("credential environment variable %s is not set", name)
	}
	return value, nil
}
