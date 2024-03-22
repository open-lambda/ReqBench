package platform_adapter

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

var ipcScript = `
BEGIN {
    printf("tid, start, duration\n");
}

kprobe:copy_ipcs {
    @ipcs_start[tid] = nsecs;
}

kretprobe:copy_ipcs /@ipcs_start[tid]/ {
    printf("%lld, %lld, %lld\n", tid, @ipcs_start[tid], nsecs - @ipcs_start[tid]);
    delete(@ipcs_start[tid]);
}

END {
    clear(@ipcs_start);
}
`

var pidScript = `
BEGIN {
    printf("tid, start, duration\n");
}

kprobe:copy_pid_ns {
    @pid_start[tid] = nsecs;
}

kretprobe:copy_pid_ns /@pid_start[tid]/ {
    printf("%lld, %lld, %lld\n", tid, @pid_start[tid], nsecs - @pid_start[tid]);
    delete(@pid_start[tid]);
}

END {
    clear(@pid_start);
}
`

var utsScript = `
BEGIN {
    printf("tid, start, duration\n");
}

kprobe:copy_utsname {
    @uts_start[tid] = nsecs;
}

kretprobe:copy_utsname /@uts_start[tid]/ {
    printf("%lld, %lld, %lld\n", tid, @uts_start[tid], nsecs - @uts_start[tid]);
    delete(@uts_start[tid]);
}

END {
    clear(@uts_start);
}
`

var newNsScript = `
BEGIN {
    printf("tid, start, duration\n");
}

kprobe:create_new_namespaces {
    @newns_start[tid] = nsecs;
}

kretprobe:create_new_namespaces /@newns_start[tid]/ {
    printf("%lld, %lld, %lld\n", tid, @newns_start[tid], nsecs - @newns_start[tid]);
    delete(@newns_start[tid]);
}

END {
    clear(@newns_start);
}
`

type subTracer struct {
	script  string
	output  string
	process *os.Process
}

type BPFTracer struct {
	tracers []subTracer
}

func NewBPFTracer(ipcOutput, pidOutput, utsOutput, newNsOutput string) *BPFTracer {
	return &BPFTracer{
		tracers: []subTracer{
			{script: ipcScript, output: ipcOutput},
			{script: pidScript, output: pidOutput},
			{script: utsScript, output: utsOutput},
			{script: newNsScript, output: newNsOutput},
		},
	}
}

func (b *BPFTracer) StartTracing() error {
	for i, tracer := range b.tracers {
		cmd := exec.Command("bpftrace", "-e", tracer.script, "-o", tracer.output)
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("bpftrace start error: %s", err)
		}
		b.tracers[i].process = cmd.Process
	}
	return nil
}

func (b *BPFTracer) StopTracing() error {
	for i, tracer := range b.tracers {
		if tracer.process != nil {
			if err := tracer.process.Signal(syscall.SIGINT); err != nil {
				return fmt.Errorf("error stopping bpftrace process (PID %lld): %s", tracer.process.Pid, err)
			}
			b.tracers[i].process = nil
		}
	}
	return nil
}
