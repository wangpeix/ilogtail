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

// case1: 多行日志 & 最后一行有换行符
func TestMultiCase1(t *testing.T) {
	var log = `[2023-05-14 14:26:18][INFO][..xiaoj
at java.base/java.lang.Thread.sleep1(Native Method)
[2023-05-14 14:26:19][INFO][..xiaoj
at java.base/java.lang.Thread.sleep2(Native Method)
at com.bigdata.agent.source.log.offset.OffsetManager$1.run(OffsetManager.java:102)
`
	processor, err := newProcessor()
	require.NoError(t, err)
	split(processor, log)
}

// case2: 多行日志 & 最后一行无换行符
func TestMultiCase2(t *testing.T) {
	var log = `[2023-05-14 14:26:18][INFO][..xiaoj
at java.base/java.lang.Thread.sleep1(Native Method)
[2023-05-14 14:26:19][INFO][..xiaoj
at java.base/java.lang.Thread.sleep2(Native Method)
at com.bigdata.agent.source.log.offset.OffsetManager$1.run(OffsetManager.java:102)`
	processor, err := newProcessor()
	require.NoError(t, err)
	split(processor, log)
}

// case3: 多行日志 & 只有一行时间规则日志 & 有换行符
func TestMultiCase3(t *testing.T) {

	var log = `[2023-05-14 14:26:18][INFO][..xiaoj
`
	processor, err := newProcessor()
	require.NoError(t, err)
	split(processor, log)
}

// case4: 多行日志 & 只有一行时间规则日志 & 无换行符
func TestMultiCase4(t *testing.T) {
	var log = `[2023-05-14 14:26:18][INFO][..xiaoj`
	processor, err := newProcessor()
	require.NoError(t, err)
	split(processor, log)
}

// case5: 多行日志 & 只有一行无时间规则日志 & 有换行符
func TestMultiCase5(t *testing.T) {
	var log = `com.bigdata.agent.source.log
`
	processor, err := newProcessor()
	require.NoError(t, err)
	split(processor, log)
}

// case6: 多行日志 & 只有一行无时间规则日志 & 无换行符
func TestMultiCase6(t *testing.T) {
	var log = `com.bigdata.agent.source.log`

	processor, err := newProcessor()
	require.NoError(t, err)
	split(processor, log)
}

// case7: 多行日志 & 只有一个换行符
func TestMultiCase7(t *testing.T) {
	var log = `
`
	processor, err := newProcessor()
	require.NoError(t, err)
	split(processor, log)
}

func split(processor *ProcessorSplitMultiRowByTimeRule, log string) {
	logPb := test.CreateLogs("content", log)
	logArray := make([]*protocol.Log, 1)
	logArray[0] = logPb

	destLogs := processor.ProcessLogs(logArray)
	marshal, _ := json.MarshalIndent(destLogs, "", "    ")
	println("processor解析后数据: " + string(marshal))
}
