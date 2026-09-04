package chunked

import (
	"context"
	"sync"
)

// DefaultTransferConcurrency is how many chunk transfers a snapshot or a
// prefetch keeps in flight.
//
// Publishing one chunk is dominated by fixed per-object latency, not by its
// bytes: a content-addressed store writes a staging file, fsyncs it, renames
// it and fsyncs the directory, and an uploaded chunk repeats that on the
// far side. Measured on APFS that is roughly 12 ms for a 64 KiB chunk, so a
// strictly serial transfer of an incompressible tree runs at a few MB/s no
// matter how fast the disk or the link is. Chunks are independent immutable
// objects, so overlapping them costs nothing in ordering and removes nothing
// from the durability of any single one.
const DefaultTransferConcurrency = 8

// transferPool runs independent chunk transfers with bounded concurrency and
// a single terminal error. It is deliberately join-based: stop cancels and
// then waits, so a caller that returns has evidence every worker exited
// rather than a request that they stop.
type transferPool struct {
	ctx    context.Context
	cancel context.CancelFunc
	jobs   chan func(context.Context) error
	wg     sync.WaitGroup

	mu   sync.Mutex
	err  error
	done bool
}

// newTransferPool starts workers workers. A non-positive workers selects
// DefaultTransferConcurrency; one worker is an ordinary serial transfer.
func newTransferPool(ctx context.Context, workers int) *transferPool {
	if workers <= 0 {
		workers = DefaultTransferConcurrency
	}
	inner, cancel := context.WithCancel(ctx)
	p := &transferPool{ctx: inner, cancel: cancel, jobs: make(chan func(context.Context) error, workers)}
	p.wg.Add(workers)
	for range workers {
		go func() {
			defer p.wg.Done()
			for job := range p.jobs {
				if err := p.ctx.Err(); err != nil {
					p.fail(err)
					continue
				}
				if err := job(p.ctx); err != nil {
					p.fail(err)
				}
			}
		}()
	}
	return p
}

// submit queues one transfer. It blocks while every worker is busy, which is
// what bounds how many chunk bodies are resident at once. It returns the
// pool's terminal error as soon as one exists so the producer stops walking
// the tree after the first failure.
func (p *transferPool) submit(job func(context.Context) error) error {
	if err := p.failure(); err != nil {
		return err
	}
	select {
	case p.jobs <- job:
		return nil
	case <-p.ctx.Done():
		if err := p.failure(); err != nil {
			return err
		}
		return p.ctx.Err()
	}
}

// wait closes the queue, joins every worker, and reports the first failure.
// It is safe to call more than once so a deferred stop can follow it.
func (p *transferPool) wait() error {
	p.mu.Lock()
	if !p.done {
		p.done = true
		close(p.jobs)
	}
	p.mu.Unlock()
	p.wg.Wait()
	return p.failure()
}

// stop cancels outstanding work and joins it. A deferred stop guarantees no
// worker is still touching the store after the caller returns.
func (p *transferPool) stop() {
	p.cancel()
	_ = p.wait()
}

func (p *transferPool) fail(err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	if p.err == nil {
		p.err = err
		p.cancel()
	}
	p.mu.Unlock()
}

func (p *transferPool) failure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}
