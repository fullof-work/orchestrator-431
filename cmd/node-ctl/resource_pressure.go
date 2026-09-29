package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

func pressureCmd(args []string) int {
	fs := flag.NewFlagSet("resource pressure", flag.ContinueOnError)
	socket := fs.String("socket", "", "orchestrator control socket (or NODE_CTL_SOCKET env)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		return 2
	}
	code, body, err := udsDo(resolveSocket(*socket), http.MethodGet, configsock.PathAdminResourcePressure, nil, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if code != http.StatusOK {
		fmt.Fprintf(os.Stderr, "resource pressure: HTTP %d: %s\n", code, apiMessage(body))
		return 1
	}
	var status nodectl.PressureStatus
	if err := json.Unmarshal(body, &status); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	out, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(string(out))
	return 0
}
