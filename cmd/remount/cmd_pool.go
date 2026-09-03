package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"remount.dev/remount/internal/client"
	"remount.dev/remount/internal/proto"
)

func cmdPool(ctx context.Context, args []string) error {
	if len(args) == 0 || isHelp(args[0]) {
		return errors.New("pool: create|ls|get|rm")
	}
	sub, rest := args[0], args[1:]
	fs := flag.NewFlagSet("pool "+sub, flag.ExitOnError)
	var commonFlags common
	commonFlags.flags(fs)
	switch sub {
	case "create":
		vendor := fs.String("vendor", "", "configured provisioner name")
		backend := fs.String("backend", "", "workspace backend run on provisioned nodes")
		minimum := fs.Int("min", 0, "minimum machines")
		maximum := fs.Int("max", 1, "maximum machines")
		idle := fs.Duration("idle-scale-down", 15*time.Minute, "destroy empty machines idle this long (0 disables)")
		region := fs.String("region", "", "provider region")
		size := fs.String("size", "", "provider machine size")
		idem := fs.String("idem", "", "stable idempotency key for retry after an ambiguous result")
		labels := kvFlag{}
		fs.Var(labels, "label", "node label k=v (repeatable)")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "pool create NAME --vendor V --backend B"); err != nil {
			return err
		}
		spec := proto.PoolSpec{
			Name: fs.Arg(0), Vendor: *vendor, Backend: *backend, Min: *minimum, Max: *maximum,
			IdleScaleDownMilli: idle.Milliseconds(), Region: *region, Size: *size, Labels: labels,
		}
		if err := proto.ValidatePoolSpec(spec); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		var options []client.OperationOption
		if *idem != "" {
			options = append(options, client.WithIdempotencyKey(*idem))
		}
		pool, err := cl.CreatePool(ctx, spec, options...)
		if err != nil {
			return err
		}
		printPool(pool, commonFlags.json)
	case "get":
		parse(fs, rest)
		if err := arity(fs, 1, 1, "pool get NAME"); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		pool, err := cl.GetPool(ctx, fs.Arg(0))
		if err != nil {
			return err
		}
		printPool(pool, commonFlags.json)
	case "ls":
		parse(fs, rest)
		if err := arity(fs, 0, 0, "pool ls"); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		pools, err := cl.ListPools(ctx)
		if err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(pools)
			return nil
		}
		tw := tabWriter()
		fmt.Fprintln(tw, "NAME\tVENDOR\tBACKEND\tCAPACITY\tCURRENT\tREGION\tLABELS")
		for i := range pools {
			pool := &pools[i]
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d-%d\t%d\t%s\t%s\n", pool.Spec.Name, pool.Spec.Vendor,
				pool.Spec.Backend, pool.Spec.Min, pool.Spec.Max, pool.Current, pool.Spec.Region, kvFlag(pool.Spec.Labels).String())
		}
		tw.Flush()
	case "rm":
		idem := fs.String("idem", "", "stable idempotency key for retry after an ambiguous result")
		parse(fs, rest)
		if err := arity(fs, 1, 1, "pool rm NAME"); err != nil {
			return err
		}
		cl := commonFlags.client()
		defer cl.Close()
		var options []client.OperationOption
		if *idem != "" {
			options = append(options, client.WithIdempotencyKey(*idem))
		}
		if err := cl.RemovePool(ctx, fs.Arg(0), options...); err != nil {
			return err
		}
		if commonFlags.json {
			printJSON(map[string]string{"removed": fs.Arg(0)})
		}
	default:
		return fmt.Errorf("unknown pool subcommand %q", sub)
	}
	return nil
}

func printPool(pool *proto.Pool, jsonOutput bool) {
	if jsonOutput {
		printJSON(pool)
		return
	}
	fmt.Printf("%s\t%s\t%s\t%d-%d\t%d\n", pool.Spec.Name, pool.Spec.Vendor, pool.Spec.Backend,
		pool.Spec.Min, pool.Spec.Max, pool.Current)
}
