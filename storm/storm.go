package storm

import (
    "bufio"
    "bytes"
    "container/list"
    "encoding/json"
    "os"
    "runtime"
    "strconv"
)

var in *bufio.Reader = bufio.NewReader(os.Stdin)
var out *bufio.Writer = bufio.NewWriter(os.Stdout)

type Spout struct{}

type BasicBolt struct{}

type Bolt struct{}

type Tuple struct {
    Id, Comp, Stream string
    Task             float64
    Value            []interface{}
}

// List of channels to goroutines waiting for Task Id responses
var waitingForTaskId *list.List = list.New()

// Stores anchor tuples for commands waiting for Task Ids to be returned
var anchorMap map[int]string = make(map[int]string)

func (b *Spout) Run(init func(map[string]interface{}, map[string]interface{}), nextTuple func()) {
    msgChan := make(chan *[]byte)
    readMsgs(msgChan)
    config := *<-msgChan
    info := unmarshal(config).(map[string]interface{})
    sendPID(info["pidDir"].(string))
    init(info["conf"].(map[string]interface{}), info["context"].(map[string]interface{}))
    for {
        jString := *<-msgChan
        go b.handleJson(jString, nextTuple)
    }
}

func (b *Spout) handleJson(jString []byte, nextTuple func()) {
    cm := unmarshal(jString).(map[string]interface{})
    command := cm["command"].(string)
    if command == "next" {
        nextTuple()
    } else if command == "ack" {
        ack(cm["id"].(string))
    } else if command == "fail" {
        fail(cm["id"].(string))
    }
    sync()
}

func (b *BasicBolt) Run(init func(map[string]interface{}, map[string]interface{}), proc func(*Tuple)) {
    msgChan := make(chan *[]byte)
    readMsgs(msgChan)
    config := *<-msgChan
    info := unmarshal(config).(map[string]interface{})
    sendPID(info["pidDir"].(string))
    init(info["conf"].(map[string]interface{}), info["context"].(map[string]interface{}))
    for {
        jString := *<-msgChan
        go b.handleJson(jString, proc)
    }
}

func (b *BasicBolt) handleJson(jString []byte, proc func(*Tuple)) {
    unM := unmarshal(jString)
    switch unM.(type) {
    case []interface{}: // Task ID
        // Task IDs are returned in same order as emitted,
        // so send to the channel at the front of the list
        pendingCommandChan := waitingForTaskId.Remove(waitingForTaskId.Front()).(chan []interface{})
        if pendingCommandChan == nil {
            panic("No pending command for task id")
        }
        pendingCommandChan <- unM.([]interface{})
    case map[string]interface{}: // Tuple
        // Custom unmarshal that conserves Tuple ID type (int64 not float64)
        unmarshalTup(jString, &unM)
        cmd := unM.(map[string]interface{})
        tup := &Tuple{
            Id:     cmd["id"].(string),
            Comp:   cmd["comp"].(string),
            Stream: cmd["stream"].(string),
            Task:   cmd["task"].(float64),
            Value:  cmd["tuple"].([]interface{}),
        }
        anchorTuple := cmd["id"].(string)
        storeAnchor(anchorTuple)
        proc(tup)
        ack(tup.Id)
    }
}

func storeAnchor(id string) {
    goId := getGoroutineId()
    anchorMap[goId] = id
}

func getAnchor() string {
    goId := getGoroutineId()
    anchor := anchorMap[goId]
    //delete(anchorMap, goId)
    return anchor
}

func getGoroutineId() int {
    buf := make([]byte, 20)
    runtime.Stack(buf, false)
    for i, v := range buf[10:20] {
        if string(v) == " " {
            gId, err := strconv.Atoi(string(buf[10 : 10+i]))
            if err != nil {
                panic("Error converting goroutine ID")
            }
            return gId
        }
    }
    panic("Error extracting goroutine ID")
}

func (b *Bolt) Run(init func(map[string]interface{}, map[string]interface{}), proc func(*Tuple)) {
    msgChan := make(chan *[]byte)
    readMsgs(msgChan)
    config := *<-msgChan
    info := unmarshal(config).(map[string]interface{})
    sendPID(info["pidDir"].(string))
    init(info["conf"].(map[string]interface{}), info["context"].(map[string]interface{}))
    for {
        jString := *<-msgChan
        go b.handleJson(jString, proc)
    }
}

func (b *Bolt) handleJson(jString []byte, proc func(*Tuple)) {
    unM := unmarshal(jString)
    switch unM.(type) {
    case []interface{}: // Task ID
        // Task IDs are returned in same order as emitted,
        // so send to the channel at the front of the list
        waitingCommand := waitingForTaskId.Remove(waitingForTaskId.Front()).(chan []interface{})
        if waitingCommand == nil {
            panic("No pending command for Task Id returned.")
        }
        waitingCommand <- unM.([]interface{})
    case map[string]interface{}: // Tuple
        // Custom unmarshal that conserves Tuple ID type (int64 not float64)
        unmarshalTup(jString, &unM)
        cmd := unM.(map[string]interface{})
        tup := &Tuple{
            Id:     cmd["id"].(string),
            Comp:   cmd["comp"].(string),
            Stream: cmd["stream"].(string),
            Task:   cmd["task"].(float64),
            Value:  cmd["tuple"].([]interface{}),
        }
        proc(tup)
        ack(tup.Id)
    }
}

func readMsgs(msgChan chan *[]byte) {
    inChan := make(chan *[]byte)
    // Constantly read from stdin into channel
    go func() {
        for {
            b, _, err := in.ReadLine()
            if err == nil {
                b_trim := make([]byte, len(b))
                copy(b_trim, b)
                inChan <- &b_trim
            }
                   }
    }()
    // Split input stream on "end" line
    go func() {
        end := []byte("end")
        for {
            line := []byte("")
            jsonIn := []byte("")
            for !bytes.Equal(line, end) {
                jsonIn = append(jsonIn, line...)
                line = *<-inChan
            }
            msgChan <- &jsonIn
        }
    }()
}

func ack(m string) {
    sendMsgToParent(map[string]interface{}{
        "command": "ack",
        "id":      m,
    })
}

func fail(m string) {
    sendMsgToParent(map[string]interface{}{
        "command": "fail",
        "id":      m,
    })
}

func sync() {
    sendMsgToParent(map[string]interface{}{
        "command": "sync",
    })
}

func Log(msg string) {
    sendMsgToParent(map[string]interface{}{
        "command": "log",
        "msg":     msg,
    })
}

func Emit(tup []interface{}) []interface{} {
    sendMsgToParent(map[string]interface{}{
        "command": "emit",
        "tuple":   tup,
        "anchors": []string{getAnchor()},
    })
    return emitGetTaskId()
}

func EmitDirect(task int, tup []interface{}) []interface{} {
    sendMsgToParent(map[string]interface{}{
        "command": "emit",
        "tuple":   tup,
        "anchors": []string{getAnchor()},
        "task":    task,
     })
    return emitGetTaskId()
}

func EmitDirectStreamAnchors(task int, tup []interface{}, stream string, anchors []string) []interface{} {
    sendMsgToParent(map[string]interface{}{
        "command": "emit",
        "tuple":   tup,
        "anchors": anchors,
        "task":    task,
        "stream":  stream,
    })
    return emitGetTaskId()
}

func EmitDirectStreamId(task int, tup []interface{}, stream string, id string) []interface{} {
    sendMsgToParent(map[string]interface{}{
        "command": "emit",
        "tuple":   tup,
        "task":    task,
        "stream":  stream,
        "id":      id,
    })
    return emitGetTaskId()
}

// Block on a channel that will contain the returned Task Id
func emitGetTaskId() []interface{} {
    tIDChan := make(chan []interface{}, 1)
    waitingForTaskId.PushBack(tIDChan)
    return <-tIDChan
} 

// Go built in JSON demarshal converts all numbers into float64.
// For very large integers, such as the Tuple Id, precision is lost.
// This work-around extracts the numbers as int64, without having
// to fully implement a json unmarshaler.
func unmarshalTup(jsonIn []byte, unM *interface{}) {
    var objMap map[string]json.RawMessage
    json.Unmarshal(jsonIn, &objMap)
    var t []int64
    json.Unmarshal(objMap["tuple"], &t)
    int64store := make(map[int]int64)
    // Store integers in tuple as int64
    for k, v := range t {
        if v != 0 {
            int64store[k] = v
        }
    }
    // Replace values from original with the correct integers
    for k, v := range int64store {
        (*unM).(map[string]interface{})["tuple"].([]interface{})[k] = v
    }
}

func sendPID(heartbeatDir string) {
    pid := os.Getpid()
    var pidMap = map[string]interface{}{
        "pid": pid,
    }
    sendMsgToParent(pidMap)

    err := os.MkdirAll(heartbeatDir+"/"+strconv.Itoa(pid), 0666)
    if err != nil {
        panic("Error creating PID directory: " + err.Error())
    }
}

func sendMsgToParent(jMap map[string]interface{}) {
    b, jErr := json.Marshal(jMap)
    if jErr != nil {
        panic("Marshal error: " + jErr.Error())
    }
    m := append(b, []byte("\nend\n")...)
    _, wErr := out.Write(m)
    if wErr != nil {
        panic("Write error")
    }
    fErr := out.Flush()
    if fErr != nil {
        panic("Flush error")
    }
}

func unmarshal(jsonIn []byte) interface{} {
    var msg interface{}
    err := json.Unmarshal(jsonIn, &msg)
    if err != nil {
        panic("Error during unmarshal")
    }
    return msg
}