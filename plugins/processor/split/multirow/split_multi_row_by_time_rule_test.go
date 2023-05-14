package multirow

import (
	"github.com/alibaba/ilogtail/plugins/test"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alibaba/ilogtail/pkg/protocol"
	"github.com/alibaba/ilogtail/plugins/test/mock"

	"encoding/json"
)

func newProcessor() (*ProcessorSplitMultiRowByTimeRule, error) {
	ctx := mock.NewEmptyContext("p", "l", "c")
	processor := &ProcessorSplitMultiRowByTimeRule{
		SplitKey:      "content",
		TimeFormat:    "YYYY-MM-DD hh:mm:ss",
		TimeStartFlag: "[",
	}
	err := processor.Init(ctx)
	return processor, err
}

func TestMultiRow(t *testing.T) {
	processor, err := newProcessor()
	require.NoError(t, err)

	log := `[2023-05-14 14:21:09][INFO][.../cache.go:198] _undef||traceid=xxx||spanid=xxx||hintCode=0||hintContent=xxx||thread=ThreadPoolExecutor-0_0||java.lang.InterruptedException: sleep interrupted
    at java.base/java.lang.Thread.sleep(Native Method)
    at java.base/java.lang.Thread.run(Thread.java:834)
[2023-05-14 14:21:10][INFO][.../cache.go:198] _undef||traceid=xxx||spanid=xxx||hintCode=0||hintContent=xxx||thread=ThreadPoolExecutor-0_0||java.lang.InterruptedException: sleep interrupted
    at java.base/java.lang.Thread.sleep(Native Method)
    at java.base/java.lang.Thread.run(Thread.java:834)`

	logPb := test.CreateLogs("content", log)
	logArray := make([]*protocol.Log, 1)
	logArray[0] = logPb

	destLogs := processor.ProcessLogs(logArray)
	marshal, _ := json.MarshalIndent(destLogs, "", "    ")
	println("processor解析后数据: " + string(marshal))

}
