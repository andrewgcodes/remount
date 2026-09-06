package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/ids"
	"remount.dev/remount/internal/launch"
	"remount.dev/remount/internal/proto"
)

// The binding command is the operator's whole credential surface: define a
// brokered credential, look at what is defined, rotate it, withdraw it. The
// secret itself is never a command-line argument. `--secret-env NAME` names an
// environment variable of *this process*, so the value stays out of the shell
// history and out of the process table every user on the box can read.

// secretFromEnv reads a credential from the named environment variable of the
// CLI process. The value is never echoed, and an empty variable is an error
// rather than a binding with an empty secret the control plane would refuse
// later with a less useful message.
func secretFromEnv(name string) (string, error) {
	if strings.TrimSpace(name) == "" {
		return "", errors.New("--secret-env needs an environment variable name")
	}
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("environment variable %s is empty: export the credential in the shell that runs this command", name)
	}
	return value, nil
}

// parseSubstitution turns `location[:name|:pointer]` into the declaration the
// control plane validates. "header" is the default and takes no argument.
func parseSubstitution(spec string) (*proto.BindingSubstitution, error) {
	if strings.TrimSpace(spec) == "" {
		return nil, nil
	}
	location, argument, hasArgument := strings.Cut(spec, ":")
	switch location {
	case proto.SubstitutionHeader:
		if hasArgument {
			return nil, fmt.Errorf("--substitution header takes no argument (got %q)", argument)
		}
		return &proto.BindingSubstitution{Location: proto.SubstitutionHeader}, nil
	case proto.SubstitutionQuery, proto.SubstitutionBodyForm:
		if !hasArgument || argument == "" {
			return nil, fmt.Errorf("--substitution %s needs a parameter name: %s:NAME", location, location)
		}
		return &proto.BindingSubstitution{Location: location, Name: argument}, nil
	case proto.SubstitutionBodyJSON:
		if !hasArgument || !strings.HasPrefix(argument, "/") {
			return nil, errors.New("--substitution body_json needs an RFC 6901 pointer: body_json:/auth/token")
		}
		return &proto.BindingSubstitution{Location: location, JSONPointer: argument}, nil
	default:
		return nil, fmt.Errorf("--substitution %q is not one of header, query:NAME, body_form:NAME, body_json:/pointer", spec)
	}
}

// upperMethods normalizes the verbs a binding is narrowed to. The control
// plane requires upper case; accepting "get" and shouting it here is kinder
// than a round trip that fails on capitalization.
func upperMethods(methods []string) []string {
	if len(methods) == 0 {
		return nil
	}
	out := make([]string, 0, len(methods))
	for _, method := range methods {
		if trimmed := strings.TrimSpace(method); trimmed != "" {
			out = append(out, strings.ToUpper(trimmed))
		}
	}
	return out
}

// substitutionLabel renders a binding's declared substitution location for the
// human table. A binding that declares nothing substitutes in a header.
func substitutionLabel(declared *proto.BindingSubstitution) string {
	if declared == nil || declared.Location == "" {
		return proto.SubstitutionHeader
	}
	switch {
	case declared.Name != "":
		return declared.Location + ":" + declared.Name
	case declared.JSONPointer != "":
		return declared.Location + ":" + declared.JSONPointer
	default:
		return declared.Location
	}
}

// bindingRow renders one binding for the human table. A secret is never part
// of a response, so there is nothing here to withhold.
func bindingRow(spec proto.BindingSpec) string {
	state := "active"
	if spec.RevokedAt != 0 {
		state = "revoked"
		if spec.RevokedReason != "" {
			state += " (" + spec.RevokedReason + ")"
		}
	}
	kind := spec.Kind
	if kind == "" {
		kind = proto.BindingKindAPIKey
	}
	return strings.Join([]string{
		spec.ID, spec.Tenant, kind, strings.Join(spec.Destinations, ","),
		substitutionLabel(spec.Substitution), fmt.Sprint(spec.Revision), state,
	}, "\t")
}

const bindingHeader = "ID\tTENANT\tKIND\tDESTINATIONS\tSUBSTITUTION\tREV\tSTATE"

func printBinding(c common, spec *proto.BindingSpec) {
	if c.json {
		printJSON(spec)
		return
	}
	tw := tabWriter()
	fmt.Fprintln(tw, bindingHeader)
	fmt.Fprintln(tw, bindingRow(*spec))
	tw.Flush()
}

func cmdBinding(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("binding: create|ls|get|rotate|revoke|preset ls|preset apply")
	}
	switch args[0] {
	case "create":
		return cmdBindingCreate(ctx, args[1:])
	case "ls":
		return cmdBindingList(ctx, args[1:])
	case "get":
		return cmdBindingGet(ctx, args[1:])
	case "rotate":
		return cmdBindingRotate(ctx, args[1:])
	case "revoke":
		return cmdBindingRevoke(ctx, args[1:])
	case "preset":
		return cmdBindingPreset(ctx, args[1:])
	default:
		return fmt.Errorf("unknown binding subcommand %q", args[0])
	}
}

func cmdBindingCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("binding create", flag.ExitOnError)
	var c common
	c.flags(fs)
	var destinations, methods, pathPrefixes, principals, workspaces listFlag
	tenant := fs.String("tenant", "", "exact tenant (defaults to caller tenant)")
	kind := fs.String("kind", proto.BindingKindAPIKey, "api_key, bearer, cookie or header")
	fs.Var(&destinations, "destination", "host the credential may be substituted for (repeatable)")
	placeholder := fs.String("placeholder", "", "the opaque string the workspace holds (default ref:ID)")
	secretEnv := fs.String("secret-env", "", "read the credential from this environment variable of this process")
	source := fs.String("source", "", "external secret reference resolved by the control plane")
	ttl := fs.Duration("ttl", 0, "lease lifetime (0 selects the server default; max 24h)")
	fs.Var(&methods, "method", "HTTP method the binding is narrowed to (repeatable)")
	fs.Var(&pathPrefixes, "path-prefix", "path prefix the binding is narrowed to (repeatable)")
	fs.Var(&principals, "principal", "principal allowed to use the binding (repeatable)")
	fs.Var(&workspaces, "workspace", "workspace allowed to use the binding (repeatable)")
	substitution := fs.String("substitution", "", "header, query:NAME, body_form:NAME or body_json:/pointer")
	noLog := fs.Bool("retention-no-log", false, "record that this binding's traffic must stay out of durable content")
	retentionNote := fs.String("retention-note", "", "free text recording the provider's retention requirement")
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 1, 1, "binding create ID --destination HOST --secret-env NAME"); err != nil {
		return err
	}
	spec := proto.BindingSpec{
		ID: fs.Arg(0), Tenant: *tenant, Kind: *kind, Source: *source,
		Destinations: destinations, Principals: principals, Workspaces: workspaces,
		Placeholder: *placeholder, TTLSec: int64(ttl.Seconds()),
		Methods: upperMethods(methods), PathPrefixes: pathPrefixes,
		Retention: proto.BindingRetention{NoLog: *noLog, Note: *retentionNote},
	}
	if *secretEnv != "" {
		secret, err := secretFromEnv(*secretEnv)
		if err != nil {
			return err
		}
		spec.Secret = secret
	}
	declared, err := parseSubstitution(*substitution)
	if err != nil {
		return err
	}
	spec.Substitution = declared
	if *idem == "" {
		*idem = ids.New("idem")
	}
	cl := c.client()
	defer cl.Close()
	created, err := cl.CreateBinding(ctx, spec, client.WithIdempotencyKey(*idem))
	if err != nil {
		return err
	}
	printBinding(c, created)
	return nil
}

func cmdBindingList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("binding ls", flag.ExitOnError)
	var c common
	c.flags(fs)
	tenant := fs.String("tenant", "", "exact tenant (defaults to caller tenant)")
	includeRevoked := fs.Bool("include-revoked", false, "include revoked bindings, which are retained for audit")
	parse(fs, args)
	if err := arity(fs, 0, 0, "binding ls [--include-revoked]"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	bindings, err := cl.ListBindings(ctx, *tenant, *includeRevoked)
	if err != nil {
		return err
	}
	if c.json {
		printJSON(bindings)
		return nil
	}
	tw := tabWriter()
	fmt.Fprintln(tw, bindingHeader)
	for _, spec := range bindings {
		fmt.Fprintln(tw, bindingRow(spec))
	}
	tw.Flush()
	return nil
}

func cmdBindingGet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("binding get", flag.ExitOnError)
	var c common
	c.flags(fs)
	tenant := fs.String("tenant", "", "exact tenant (defaults to caller tenant)")
	parse(fs, args)
	if err := arity(fs, 1, 1, "binding get ID"); err != nil {
		return err
	}
	cl := c.client()
	defer cl.Close()
	spec, err := cl.GetBinding(ctx, *tenant, fs.Arg(0))
	if err != nil {
		return err
	}
	printBinding(c, spec)
	return nil
}

func cmdBindingRotate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("binding rotate", flag.ExitOnError)
	var c common
	c.flags(fs)
	tenant := fs.String("tenant", "", "exact tenant (defaults to caller tenant)")
	secretEnv := fs.String("secret-env", "", "read the new credential from this environment variable of this process")
	source := fs.String("source", "", "external secret reference resolved by the control plane")
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 1, 1, "binding rotate ID --secret-env NAME"); err != nil {
		return err
	}
	req := proto.BindingRotateReq{ID: fs.Arg(0), Tenant: *tenant, Source: *source}
	if *secretEnv != "" {
		secret, err := secretFromEnv(*secretEnv)
		if err != nil {
			return err
		}
		req.Secret = secret
	}
	if *idem == "" {
		*idem = ids.New("idem")
	}
	cl := c.client()
	defer cl.Close()
	spec, err := cl.RotateBinding(ctx, req, client.WithIdempotencyKey(*idem))
	if err != nil {
		return err
	}
	printBinding(c, spec)
	return nil
}

func cmdBindingRevoke(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("binding revoke", flag.ExitOnError)
	var c common
	c.flags(fs)
	tenant := fs.String("tenant", "", "exact tenant (defaults to caller tenant)")
	reason := fs.String("reason", "", "why the binding was withdrawn; recorded in binding.revoked")
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 1, 1, "binding revoke ID [--reason TEXT]"); err != nil {
		return err
	}
	if *idem == "" {
		*idem = ids.New("idem")
	}
	cl := c.client()
	defer cl.Close()
	spec, err := cl.RevokeBinding(ctx, *tenant, fs.Arg(0), *reason, client.WithIdempotencyKey(*idem))
	if err != nil {
		return err
	}
	printBinding(c, spec)
	if !c.json {
		fmt.Fprintln(os.Stderr, "revocation stops Remount substituting this credential within one renew interval;")
		fmt.Fprintln(os.Stderr, "it is not provider-side revocation - rotate or delete the key at the provider too.")
	}
	return nil
}

func cmdBindingPreset(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("binding preset: ls|apply PRESET")
	}
	switch args[0] {
	case "ls":
		return cmdBindingPresetList(args[1:])
	case "apply":
		return cmdBindingPresetApply(ctx, args[1:])
	default:
		return fmt.Errorf("unknown binding preset subcommand %q", args[0])
	}
}

func cmdBindingPresetList(args []string) error {
	fs := flag.NewFlagSet("binding preset ls", flag.ExitOnError)
	var c common
	c.flags(fs)
	parse(fs, args)
	if err := arity(fs, 0, 0, "binding preset ls"); err != nil {
		return err
	}
	presets := launch.Presets()
	if c.json {
		printJSON(presets)
		return nil
	}
	tw := tabWriter()
	fmt.Fprintln(tw, "PRESET\tHOSTS\tKEY ENV\tBASE URL ENV\tHEADER\tNOTE")
	for _, p := range presets {
		hosts := strings.Join(p.Hosts, ",")
		if p.HostParam != "" {
			hosts += " (?host=" + p.HostParam + ")"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", p.Name, hosts, p.KeyEnv, p.BaseURLEnv, p.Header, p.Note)
	}
	tw.Flush()
	return nil
}

// presetBindingID is the conventional id for a preset's binding. `--binding
// b_openai` resolves a bare id to the preset named by its suffix, so this
// keeps the one-command path and the recipe path naming the same thing.
func presetBindingID(preset string) string {
	return "b_" + strings.ReplaceAll(preset, "-", "_")
}

// cmdBindingPresetApply is the one-command form of the common case. The
// provider shapes already live in internal/launch, so `binding preset apply
// openai --secret-env OPENAI_API_KEY` is exactly the create call a reader
// would otherwise assemble by hand from the provider's documentation.
func cmdBindingPresetApply(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("binding preset apply", flag.ExitOnError)
	var c common
	c.flags(fs)
	tenant := fs.String("tenant", "", "exact tenant (defaults to caller tenant)")
	id := fs.String("id", "", "binding id (default b_<preset>, which --binding resolves without a suffix)")
	secretEnv := fs.String("secret-env", "", "read the credential from this environment variable (default the preset's key variable)")
	source := fs.String("source", "", "external secret reference resolved by the control plane")
	host := fs.String("host", "", "deployment host component for presets that need one (azure-openai, bedrock)")
	placeholder := fs.String("placeholder", "", "the opaque string the workspace holds (default ref:ID)")
	ttl := fs.Duration("ttl", 0, "lease lifetime (0 selects the server default; max 24h)")
	idem := fs.String("idem", "", "stable idempotency key")
	parse(fs, args)
	if err := arity(fs, 1, 1, "binding preset apply PRESET [--secret-env NAME]"); err != nil {
		return err
	}
	name := fs.Arg(0)
	preset, ok := launch.LookupPreset(name)
	if !ok {
		return fmt.Errorf("no provider preset %q (have %s)", name, strings.Join(launch.PresetNames(), ", "))
	}
	if *id == "" {
		*id = presetBindingID(preset.Name)
	}
	query := ""
	if *host != "" {
		query = "?host=" + *host
	}
	binding, err := launch.ParseBinding(*id + ":" + preset.Name + query)
	if err != nil {
		return err
	}
	spec := proto.BindingSpec{
		ID: binding.ID, Tenant: *tenant, Kind: proto.BindingKindAPIKey, Source: *source,
		Destinations: binding.Hosts(), Placeholder: *placeholder, TTLSec: int64(ttl.Seconds()),
	}
	if *source == "" {
		if *secretEnv == "" {
			*secretEnv = preset.KeyEnv
		}
		secret, secretErr := secretFromEnv(*secretEnv)
		if secretErr != nil {
			return secretErr
		}
		spec.Secret = secret
	}
	if *idem == "" {
		*idem = ids.New("idem")
	}
	cl := c.client()
	defer cl.Close()
	created, err := cl.CreateBinding(ctx, spec, client.WithIdempotencyKey(*idem))
	if err != nil {
		return err
	}
	if c.json {
		printJSON(map[string]any{
			"binding": created, "preset": preset.Name, "spec": binding.String(),
			"key_env": preset.KeyEnv, "base_url_env": preset.BaseURLEnv,
			"base_url": "${REMOUNT_BROKER}" + binding.BaseURLPath(),
		})
		return nil
	}
	printBinding(c, created)
	fmt.Fprintf(os.Stderr, "attach with: remount ws create --binding %s --env %s=ref:%s --env %s='${REMOUNT_BROKER}%s'\n",
		binding.String(), preset.KeyEnv, binding.ID, preset.BaseURLEnv, binding.BaseURLPath())
	return nil
}

// sessionPrincipalOptions are the flags `principal session` adds. They live
// here with the rest of the credential surface rather than in the identity
// command, which owns long-lived principals.
type sessionPrincipalOptions struct {
	ws      string
	subject string
	roles   string
	ttl     time.Duration
	idem    string
}

func (o *sessionPrincipalOptions) flags(fs *flag.FlagSet) {
	fs.StringVar(&o.ws, "ws", "", "workspace the capability is bound to (required)")
	fs.StringVar(&o.subject, "subject", "", "principal id to mint (default generated)")
	fs.StringVar(&o.roles, "roles", "agent", "comma-separated roles")
	fs.DurationVar(&o.ttl, "ttl", 0, "capability lifetime (0 selects the server default; max 1h)")
	fs.StringVar(&o.idem, "idem", "", "stable idempotency key")
}

// runPrincipalSession mints an ephemeral principal and the workspace- and
// generation-bound capability it acts with, then prints the bearer once. The
// token is never stored: a replay of the same idempotency key re-mints it
// rather than returning one from durable state.
func runPrincipalSession(ctx context.Context, c common, tenant string, o sessionPrincipalOptions) error {
	if strings.TrimSpace(o.ws) == "" {
		return errors.New("principal session needs --ws WS")
	}
	if o.idem == "" {
		o.idem = ids.New("idem")
	}
	cl := c.client()
	defer cl.Close()
	res, err := cl.CreateSessionPrincipal(ctx, proto.PrincipalSessionCreateReq{
		Tenant: tenant, Subject: o.subject, Roles: splitList(o.roles),
		Workspace: o.ws, TTLSec: int64(o.ttl.Seconds()), IdempotencyKey: o.idem,
	})
	if err != nil {
		return err
	}
	expires := time.UnixMilli(res.ExpiresAt).UTC().Format(time.RFC3339)
	if c.json {
		printJSON(map[string]any{
			"principal": res.Principal, "token": res.Token, "ws": res.Workspace,
			"gen": res.Generation, "expires_at": expires,
		})
		return nil
	}
	fmt.Println(res.Token)
	fmt.Fprintf(os.Stderr, "principal %s for %s generation %d, expires %s (the token is shown once)\n",
		res.Principal.ID, res.Workspace, res.Generation, expires)
	return nil
}
