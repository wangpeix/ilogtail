// Copyright 2021 iLogtail Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multirow

import (
	"github.com/alibaba/ilogtail/pkg/pipeline"
	"github.com/alibaba/ilogtail/pkg/protocol"
	"strings"
	"time"
)

type ProcessorSplitMultiRowByTimeRule struct {
	TimeStartFlag    string
	TimeFormat       string
	TimeFormatLength int
	SplitKey         string
	context          pipeline.Context
}

const pluginName = "processor_split_multi_row_by_time_rule"

// Init called for init some system resources, like socket, mutex...
func (p *ProcessorSplitMultiRowByTimeRule) Init(context pipeline.Context) error {
	p.context = context
	p.TimeFormatLength = len(p.TimeFormat)
	return nil
}

func (*ProcessorSplitMultiRowByTimeRule) Description() string {
	return "split multi row by timerule"
}

func (p *ProcessorSplitMultiRowByTimeRule) ProcessLogs(logArray []*protocol.Log) []*protocol.Log {
	destArray := make([]*protocol.Log, 0, len(logArray))

	for _, log := range logArray {
		newLog := &protocol.Log{}
		var destCont *protocol.Log_Content
		for _, cont := range log.Contents {
			if destCont == nil && (len(p.SplitKey) != 0 && cont.Key == p.SplitKey) {
				destCont = cont
			}
		}
		if log.Time != uint32(0) {
			newLog.Time = log.Time
		} else {
			newLog.Time = (uint32)(time.Now().Unix())
		}
		if destCont != nil {
			destArray = p.SplitLog(destArray, newLog, destCont)
		}
	}

	return destArray
}

func (p *ProcessorSplitMultiRowByTimeRule) SplitLog(destLogArray []*protocol.Log, destLog *protocol.Log, rowContent *protocol.Log_Content) []*protocol.Log {
	valueStr := rowContent.GetValue()

	lastCheckIndex := 0

	isFirstLine := true
	var lastLineContent strings.Builder
	totalLen := len(valueStr)
	for i := 0; i < totalLen; i++ {
		if (valueStr[i] == '\n') || (i == (totalLen-1) && valueStr[i] != '\n') {
			line := valueStr[lastCheckIndex : i+1]
			timeString := p.getTimeString(line)
			if timeString != nil {
				if isFirstLine {
					lastLineContent.WriteString(line)
					isFirstLine = false

					if i == totalLen-1 {
						copyLog := protocol.CloneLog(destLog)
						copyLog.Contents = append(copyLog.Contents, &protocol.Log_Content{
							Key: rowContent.GetKey(), Value: lastLineContent.String()})
						destLogArray = append(destLogArray, copyLog)
						lastLineContent.Reset()
					}
				} else {
					copyLog := protocol.CloneLog(destLog)
					copyLog.Contents = append(copyLog.Contents, &protocol.Log_Content{
						Key: rowContent.GetKey(), Value: lastLineContent.String()})
					destLogArray = append(destLogArray, copyLog)

					lastLineContent.Reset()
					lastLineContent.WriteString(line)
				}
			} else {
				if !isFirstLine {
					lastLineContent.WriteString(line)
					if i == totalLen-1 {
						copyLog := protocol.CloneLog(destLog)
						copyLog.Contents = append(copyLog.Contents, &protocol.Log_Content{
							Key: rowContent.GetKey(), Value: lastLineContent.String()})
						destLogArray = append(destLogArray, copyLog)
						lastLineContent.Reset()
					}
				} else {
					lastLineContent.WriteString(line)
					isFirstLine = false

					if i == totalLen-1 {
						copyLog := protocol.CloneLog(destLog)
						copyLog.Contents = append(copyLog.Contents, &protocol.Log_Content{
							Key: rowContent.GetKey(), Value: lastLineContent.String()})
						destLogArray = append(destLogArray, copyLog)
						lastLineContent.Reset()
					}

				}
			}
			lastCheckIndex = i + 1
		}
	}

	return destLogArray
}

func (p *ProcessorSplitMultiRowByTimeRule) getTimeString(line string) *string {
	if len(line) < p.TimeFormatLength {
		return nil
	}

	index := strings.Index(line, p.TimeStartFlag)
	if index >= 0 {
		if (index + 1) >= (p.TimeFormatLength + 1) {
			return nil
		}
		timeString := line[index+1 : p.TimeFormatLength+1]
		return &timeString
	}

	return nil
}

func init() {
	pipeline.Processors[pluginName] = func() pipeline.Processor {
		return &ProcessorSplitMultiRowByTimeRule{
			TimeStartFlag: "timestamp=",
			TimeFormat:    "YYYY-MM-DD hh:mm:ss",
			SplitKey:      "content",
		}
	}
}
