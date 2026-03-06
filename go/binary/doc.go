// doc.go
// Package binary provides lifecycle management for executable subprocesses.
//
// [Binary] wraps [os/exec] with structured logging, port readiness checks,
// graceful shutdown via SIGTERM, and asynchronous exit notification.
//
// A binary can be run synchronously or asynchronously:
//
//	// Synchronous: blocks until the process exits.
//	err := binary.MustNew("migrate", "--up").Run()
//
//	// Asynchronous: returns once the process is running and ready.
//	server := binary.MustNew("myserver").WithPort(8080)
//	server.OnExit(func(err error) { log.Fatal(err) })
//	if err := server.Start(); err != nil { ... }
//	defer server.Stop()
//
// [Worker] orchestrates multiple binaries, coordinating startup, error
// propagation, and shutdown:
//
//	w := binary.NewWorker("myapp", binaries)
//	go func() { <-sigChan; w.Stop() }()
//	if err := w.Run(); err != nil { ... }
package binary
