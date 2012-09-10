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
