//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package rpc_utils

import (
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"github.com/stretchr/testify/assert"
	"os"
	//	"io"
	"testing"
)

func init() {
	log.SetLevel(log.LEVEL_INFO)
	log.SetFlags(log.Flags() | log.Lshortfile)
}

//
// go test proxy -v -run "TestSocketPermissionChange"
//
func TestSocketPermissionChange(t *testing.T) {

	socketFile := "aaa.sock"
	if FileExist(socketFile) {
		os.Remove(socketFile)
	}
	s, _ := NewTServerUnixDomain(socketFile)
	s.Listen()

	assert.True(t, true)

}
