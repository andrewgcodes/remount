package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/proto"
)

func cmdTenant(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("tenant: create|ls")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("tenant "+sub, flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	switch sub {
	case "create":
		idem := fs.String("idem", "", "stable idempotency key")
		oidcFile := fs.String("oidc", "", "tenant OIDC JSON configuration")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "tenant create TENANT"); err != nil {
			return err
		}
		if *idem == "" {
			*idem = ids.New("idem")
		}
		cl := commonFlags.client()
		defer cl.Close()
		policy := proto.TenantPolicy{}
		if *oidcFile != "" {
			raw, err := os.ReadFile(*oidcFile)
			if err != nil {
				return err
			}
			decoder := json.NewDecoder(strings.NewReader(string(raw)))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&policy.OIDC); err != nil {
				return fmt.Errorf("decode --oidc: %w", err)
			}
		}
		value, err := cl.CreateTenant(ctx, fs.Arg(0), policy, *idem)
		if err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(value)
		} else {
			fmt.Println(value.ID)
		}
	case "ls":
		parse(fs, rest)
		if err := arity(fs, 0, 0, "tenant ls"); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		values, err := cl.ListTenants(ctx)
		if err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(values)
			return nil
		}
		for _, value := range values {
			fmt.Printf("%s\t%s\t%d\n", value.ID, value.State, value.Revision)
		}
	default:
		return fmt.Errorf("unknown tenant subcommand %q", sub)
	}
	return nil
}

func cmdPrincipal(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("principal: create|ls|revoke")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("principal "+sub, flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	tenant := fs.String("tenant", "", "exact tenant (defaults to caller tenant)")
	switch sub {
	case "create":
		roles := fs.String("roles", "agent", "comma-separated roles")
		idem := fs.String("idem", "", "stable idempotency key")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "principal create PRINCIPAL [--tenant TENANT] [--roles agent]"); err != nil {
			return err
		}
		if *idem == "" {
			*idem = ids.New("idem")
		}
		cl := commonFlags.client()
		defer cl.Close()
		value, err := cl.CreatePrincipal(ctx, *tenant, fs.Arg(0), splitList(*roles), *idem)
		if err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(value)
		} else {
			fmt.Printf("%s\t%s\t%s\n", value.Tenant, value.ID, strings.Join(value.Roles, ","))
		}
	case "ls":
		parse(fs, rest)
		if err := arity(fs, 0, 0, "principal ls [--tenant TENANT]"); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		values, err := cl.ListPrincipals(ctx, *tenant)
		if err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(values)
			return nil
		}
		for _, value := range values {
			fmt.Printf("%s\t%s\t%s\t%d\n", value.Tenant, value.ID, strings.Join(value.Roles, ","), value.Revision)
		}
	case "revoke":
		idem := fs.String("idem", "", "stable idempotency key")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "principal revoke PRINCIPAL [--tenant TENANT]"); err != nil {
			return err
		}
		if *idem == "" {
			*idem = ids.New("idem")
		}
		cl := commonFlags.client()
		defer cl.Close()
		revision, err := cl.RevokePrincipal(ctx, *tenant, fs.Arg(0), *idem)
		if err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(map[string]any{"principal": fs.Arg(0), "revision": revision})
		} else {
			fmt.Println(revision)
		}
	default:
		return fmt.Errorf("unknown principal subcommand %q", sub)
	}
	return nil
}

func cmdToken(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "issue" {
		return errors.New("token: issue PRINCIPAL --role agent --ttl 1h [--tenant TENANT]")
	}
	fs := flag.NewFlagSet("token issue", flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	tenant := fs.String("tenant", "", "exact tenant (defaults to caller tenant)")
	role := fs.String("role", "agent", "one assigned role")
	ttl := fs.Duration("ttl", time.Hour, "access token lifetime (1s to 24h)")
	idem := fs.String("idem", "", "stable request identity")
	parse(fs, args[1:])
	if err := arity(fs, 1, 1, "token issue PRINCIPAL --role agent --ttl 1h"); err != nil {
		return err
	}
	if *idem == "" {
		*idem = ids.New("idem")
	}
	cl := commonFlags.client()
	defer cl.Close()
	result, err := cl.IssuePrincipalToken(ctx, *tenant, fs.Arg(0), *role, *ttl, *idem)
	if err != nil {
		return err
	}
	if commonFlags.json {
		printJSON(result)
	} else {
		fmt.Println(result.AccessToken)
	}
	return nil
}

func cmdInvite(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("invite", flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	tenant := fs.String("tenant", "", "tenant the operator is bound to")
	ttl := fs.Duration("ttl", 15*time.Minute, "initial access lifetime")
	idem := fs.String("idem", "", "stable request identity")
	parse(fs, args)
	if err := arity(fs, 1, 1, "invite PRINCIPAL --tenant TENANT"); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("invite requires --tenant")
	}
	if *idem == "" {
		*idem = ids.New("idem")
	}
	cl := commonFlags.client()
	defer cl.Close()
	result, err := cl.InvitePrincipal(ctx, *tenant, fs.Arg(0), *ttl, *idem)
	if err != nil {
		return err
	}
	if commonFlags.json {
		printJSON(result)
	} else {
		fmt.Println(result.AccessToken)
	}
	return nil
}
